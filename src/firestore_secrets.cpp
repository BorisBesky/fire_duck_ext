#include "firestore_secrets.hpp"
#include "firestore_logger.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace duckdb {

// Secret type name
static constexpr const char *FIRESTORE_SECRET_TYPE = "firestore";
static constexpr const char *FIRESTORE_ENV_SECRET_NAME = "__firestore_env__";

// Credentials cache - keeps credentials with their access tokens alive across queries
static std::unordered_map<std::string, std::shared_ptr<FirestoreCredentials>> credentials_cache;
static std::mutex credentials_cache_mutex;

static void PurgeSecretCredentialsCache() {
	std::lock_guard<std::mutex> lock(credentials_cache_mutex);
	int removed = 0;
	for (auto it = credentials_cache.begin(); it != credentials_cache.end();) {
		if (it->first.rfind("secret:", 0) == 0) {
			it = credentials_cache.erase(it);
			removed++;
			continue;
		}
		++it;
	}
	if (removed > 0) {
		FS_LOG_DEBUG("Secret credentials cache purged: " + std::to_string(removed) + " entries removed");
	}
}

void ClearFirestoreCredentialsCache() {
	std::lock_guard<std::mutex> lock(credentials_cache_mutex);
	if (credentials_cache.empty()) {
		return;
	}
	int removed = static_cast<int>(credentials_cache.size());
	credentials_cache.clear();
	FS_LOG_DEBUG("Credentials cache cleared: " + std::to_string(removed) + " entries removed");
}

void EnsureFirestoreEnvSecret(DatabaseInstance &db) {
	const char *env_creds = std::getenv("GOOGLE_APPLICATION_CREDENTIALS");
	if (!env_creds || strlen(env_creds) == 0) {
		return;
	}

	auto &secret_manager = SecretManager::Get(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);

	// If the env secret already exists, don't override it.
	auto existing = secret_manager.GetSecretByName(transaction, FIRESTORE_ENV_SECRET_NAME);
	if (existing) {
		return;
	}

	try {
		auto creds = FirestoreAuthManager::LoadServiceAccount(env_creds);
		auto secret =
		    make_uniq<KeyValueSecret>(vector<string> {}, FIRESTORE_SECRET_TYPE, "env", FIRESTORE_ENV_SECRET_NAME);
		secret->secret_map["project_id"] = creds->project_id;
		secret->secret_map["database_id"] = Value("(default)");
		secret->secret_map["service_account_json"] = std::string(env_creds);
		secret->secret_map["auth_type"] = Value("service_account");

		secret_manager.RegisterSecret(transaction, std::move(secret), OnCreateConflict::IGNORE_ON_CONFLICT,
		                              SecretPersistType::TEMPORARY);
		FS_LOG_DEBUG("Env Firestore secret registered: " + std::string(FIRESTORE_ENV_SECRET_NAME));
	} catch (const std::exception &ex) {
		FS_LOG_WARN("Failed to register env Firestore secret: " + std::string(ex.what()));
	}
}

// Session-scoped connected database storage
// Maps ClientContext pointer to the connected database_id
static std::unordered_map<const ClientContext *, std::string> connected_databases;
static std::mutex connected_db_mutex;

// Create secret from configuration options
static unique_ptr<BaseSecret> CreateFirestoreSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto result = make_uniq<KeyValueSecret>(scope, FIRESTORE_SECRET_TYPE, "config", input.name);

	// Get project_id (required)
	auto project_id = input.options.find("project_id");
	if (project_id == input.options.end()) {
		throw InvalidInputException("firestore secret requires 'project_id'");
	}
	result->secret_map["project_id"] = project_id->second.ToString();

	// Get database_id (optional, defaults to "(default)")
	auto database_id = input.options.find("database");
	if (database_id != input.options.end()) {
		result->secret_map["database_id"] = database_id->second.ToString();
	} else {
		result->secret_map["database_id"] = Value("(default)");
	}

	// Check for service account JSON path
	auto sa_json = input.options.find("service_account_json");
	if (sa_json != input.options.end()) {
		result->secret_map["service_account_json"] = sa_json->second.ToString();
		result->secret_map["auth_type"] = Value("service_account");
		return std::move(result);
	}

	// Check for API key
	auto api_key = input.options.find("api_key");
	if (api_key != input.options.end()) {
		result->secret_map["api_key"] = api_key->second.ToString();
		result->secret_map["auth_type"] = Value("api_key");
		return std::move(result);
	}

	throw InvalidInputException("firestore secret requires either 'service_account_json' or 'api_key'");
}

// Deserialize secret from storage
static unique_ptr<BaseSecret> FirestoreSecretDeserialize(Deserializer &deserializer, BaseSecret base_secret) {
	return KeyValueSecret::Deserialize<KeyValueSecret>(deserializer, std::move(base_secret));
}

void RegisterFirestoreSecretType(ExtensionLoader &loader) {
	// Register secret type
	SecretType secret_type;
	secret_type.name = FIRESTORE_SECRET_TYPE;
	secret_type.deserializer = FirestoreSecretDeserialize;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	// Register config provider with named parameters
	CreateSecretFunction config_function;
	config_function.secret_type = FIRESTORE_SECRET_TYPE;
	config_function.provider = "config";
	config_function.function = CreateFirestoreSecretFromConfig;

	// Declare the parameters this secret type accepts
	config_function.named_parameters["project_id"] = LogicalType::VARCHAR;
	config_function.named_parameters["service_account_json"] = LogicalType::VARCHAR;
	config_function.named_parameters["api_key"] = LogicalType::VARCHAR;
	config_function.named_parameters["database"] = LogicalType::VARCHAR;

	loader.RegisterFunction(config_function);
}

// ============================================================================
// Session-scoped database connection management
// ============================================================================

std::optional<std::string> GetConnectedDatabase(ClientContext &context) {
	std::lock_guard<std::mutex> lock(connected_db_mutex);
	auto it = connected_databases.find(&context);
	if (it != connected_databases.end()) {
		return it->second;
	}
	return std::nullopt;
}

void SetConnectedDatabase(ClientContext &context, const std::string &database_id) {
	std::lock_guard<std::mutex> lock(connected_db_mutex);
	connected_databases[&context] = database_id;
	FS_LOG_DEBUG("Connected to database: " + database_id);
}

void ClearConnectedDatabase(ClientContext &context) {
	std::lock_guard<std::mutex> lock(connected_db_mutex);
	connected_databases.erase(&context);
	FS_LOG_DEBUG("Disconnected from database");
}

bool DatabaseMatchesSecret(const Value &secret_db_value, const std::string &target_db) {
	// Handle VARCHAR type (single value or wildcard)
	if (secret_db_value.type().id() == LogicalTypeId::VARCHAR) {
		std::string db_str = secret_db_value.ToString();
		// Wildcard matches all databases
		if (db_str == "*") {
			return true;
		}
		// Exact match
		return db_str == target_db;
	}

	// Handle LIST type (array of databases)
	if (secret_db_value.type().id() == LogicalTypeId::LIST) {
		auto &list = ListValue::GetChildren(secret_db_value);
		for (const auto &item : list) {
			if (item.ToString() == target_db) {
				return true;
			}
		}
		return false;
	}

	// Unknown type - no match
	return false;
}

std::shared_ptr<FirestoreCredentials>
GetFirestoreCredentialsFromSecret(ClientContext &context, const std::string &secret_name,
                                  const std::optional<std::string> &target_database) {
	auto &secret_manager = SecretManager::Get(context);

	// Try to find a firestore secret
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, "firestore", FIRESTORE_SECRET_TYPE);

	if (!secret_match.HasMatch()) {
		PurgeSecretCredentialsCache();
		return nullptr;
	}

	auto &secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());

	// If target database is specified, check if this secret matches
	if (target_database.has_value()) {
		auto database_id_it = secret.secret_map.find("database_id");
		if (database_id_it != secret.secret_map.end()) {
			if (!DatabaseMatchesSecret(database_id_it->second, target_database.value())) {
				// This secret doesn't match the target database
				FS_LOG_DEBUG("Secret doesn't match target database: " + target_database.value());
				return nullptr;
			}
		} else {
			// No database_id in secret means it only works for "(default)"
			if (target_database.value() != "(default)") {
				return nullptr;
			}
		}
	}

	// Get project ID
	auto project_id_it = secret.secret_map.find("project_id");
	if (project_id_it == secret.secret_map.end()) {
		throw InvalidInputException("Firestore secret missing project_id");
	}
	std::string project_id = project_id_it->second.ToString();

	// Get auth type
	auto auth_type_it = secret.secret_map.find("auth_type");
	if (auth_type_it == secret.secret_map.end()) {
		throw InvalidInputException("Firestore secret missing auth_type");
	}
	std::string auth_type = auth_type_it->second.ToString();

	// Get database_id - use target_database if specified, otherwise from secret
	std::string database_id = target_database.value_or("(default)");
	auto database_id_it = secret.secret_map.find("database_id");
	if (!target_database.has_value() && database_id_it != secret.secret_map.end()) {
		// If no target specified, use the secret's database_id (if it's a single value)
		if (database_id_it->second.type().id() == LogicalTypeId::VARCHAR) {
			std::string db_val = database_id_it->second.ToString();
			if (db_val != "*") {
				database_id = db_val;
			}
		}
	}

	// Check cache for secret-based credentials (keeps access token alive across queries)
	std::string cache_key;
	if (auth_type == "service_account") {
		auto sa_json_it = secret.secret_map.find("service_account_json");
		if (sa_json_it == secret.secret_map.end()) {
			throw InvalidInputException("Firestore secret missing service_account_json");
		}
		cache_key = "secret:service_account:" + sa_json_it->second.ToString() + ":" + database_id;
	} else if (auth_type == "api_key") {
		auto api_key_it = secret.secret_map.find("api_key");
		if (api_key_it == secret.secret_map.end()) {
			throw InvalidInputException("Firestore secret missing api_key");
		}
		cache_key = "secret:api_key:" + project_id + ":" + database_id + ":" + api_key_it->second.ToString();
	}

	if (!cache_key.empty()) {
		std::lock_guard<std::mutex> lock(credentials_cache_mutex);
		auto it = credentials_cache.find(cache_key);
		if (it != credentials_cache.end()) {
			auto refreshed_match = secret_manager.LookupSecret(transaction, "firestore", FIRESTORE_SECRET_TYPE);
			if (!refreshed_match.HasMatch()) {
				PurgeSecretCredentialsCache();
				return nullptr;
			}
			FS_LOG_DEBUG("Credentials cache hit for: " + cache_key);
			return it->second;
		}
	}

	std::shared_ptr<FirestoreCredentials> creds;
	if (auth_type == "service_account") {
		auto sa_json_it = secret.secret_map.find("service_account_json");
		if (sa_json_it == secret.secret_map.end()) {
			throw InvalidInputException("Firestore secret missing service_account_json");
		}
		creds = FirestoreAuthManager::LoadServiceAccount(sa_json_it->second.ToString());
	} else if (auth_type == "api_key") {
		auto api_key_it = secret.secret_map.find("api_key");
		if (api_key_it == secret.secret_map.end()) {
			throw InvalidInputException("Firestore secret missing api_key");
		}
		creds = FirestoreAuthManager::CreateApiKeyCredentials(project_id, api_key_it->second.ToString());
	} else {
		throw InvalidInputException("Unknown firestore auth_type: " + auth_type);
	}

	// Set database_id on credentials
	creds->database_id = database_id;

	// Store secret-based credentials in cache
	if (!cache_key.empty()) {
		std::lock_guard<std::mutex> lock(credentials_cache_mutex);
		credentials_cache[cache_key] = creds;
		FS_LOG_DEBUG("Credentials cached for: " + cache_key);
	}
	return creds;
}

std::shared_ptr<FirestoreCredentials> ResolveFirestoreCredentials(ClientContext &context,
                                                                  const std::optional<std::string> &project_id,
                                                                  const std::optional<std::string> &credentials_path,
                                                                  const std::optional<std::string> &api_key,
                                                                  const std::optional<std::string> &database_id) {
	// Determine effective database_id:
	// 1. Explicit database_id parameter takes priority
	// 2. Fall back to connected database (from firestore_connect)
	// 3. Otherwise use default behavior (from secret or "(default)")
	std::optional<std::string> effective_database_id = database_id;
	if (!effective_database_id.has_value()) {
		effective_database_id = GetConnectedDatabase(context);
	}

	// Only cache credentials from file paths here; secret-based caching is handled
	// inside GetFirestoreCredentialsFromSecret.
	// Cache key includes database_id to avoid returning wrong database for cached credentials.
	std::string cache_key;
	bool should_cache = false;
	std::string db_suffix = effective_database_id.value_or("(default)");

	if (credentials_path.has_value()) {
		cache_key = "path:" + credentials_path.value() + ":" + db_suffix;
		should_cache = true;
	}

	// Check cache first (only for file-based credentials)
	if (should_cache) {
		std::lock_guard<std::mutex> lock(credentials_cache_mutex);
		auto it = credentials_cache.find(cache_key);
		if (it != credentials_cache.end()) {
			FS_LOG_DEBUG("Credentials cache hit for: " + cache_key);
			return it->second;
		}
	}

	std::shared_ptr<FirestoreCredentials> creds;

	// Priority 1: Explicit credentials path
	if (credentials_path.has_value()) {
		creds = FirestoreAuthManager::LoadServiceAccount(credentials_path.value());
	}
	// Priority 2: Explicit API key (requires project_id)
	else if (api_key.has_value()) {
		if (!project_id.has_value()) {
			throw InvalidInputException("api_key requires project_id parameter");
		}
		creds = FirestoreAuthManager::CreateApiKeyCredentials(project_id.value(), api_key.value());
	}
	// Priority 3: DuckDB secret manager (with database filtering)
	else {
		creds = GetFirestoreCredentialsFromSecret(context, "", effective_database_id);
	}

	// No credentials found
	if (!creds) {
		return nullptr;
	}

	// Apply effective_database_id (overrides secret value)
	if (effective_database_id.has_value()) {
		creds->database_id = effective_database_id.value();
	}

	// Store in cache (only for file-based credentials that need token refresh)
	if (should_cache) {
		std::lock_guard<std::mutex> lock(credentials_cache_mutex);
		credentials_cache[cache_key] = creds;
		FS_LOG_DEBUG("Credentials cached for: " + cache_key);
	}

	return creds;
}

} // namespace duckdb
