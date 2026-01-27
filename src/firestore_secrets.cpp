#include "firestore_secrets.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include <cstdlib>

namespace duckdb {

// Secret type name
static constexpr const char *FIRESTORE_SECRET_TYPE = "firestore";

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

std::shared_ptr<FirestoreCredentials> GetFirestoreCredentialsFromSecret(ClientContext &context,
                                                                        const std::string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);

	// Try to find a firestore secret
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, "firestore", FIRESTORE_SECRET_TYPE);

	if (!secret_match.HasMatch()) {
		return nullptr;
	}

	auto &secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());

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

	// Get database_id (optional, defaults to "(default)")
	std::string database_id = "(default)";
	auto database_id_it = secret.secret_map.find("database_id");
	if (database_id_it != secret.secret_map.end()) {
		database_id = database_id_it->second.ToString();
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
	return creds;
}

std::shared_ptr<FirestoreCredentials> ResolveFirestoreCredentials(ClientContext &context,
                                                                  const std::optional<std::string> &project_id,
                                                                  const std::optional<std::string> &credentials_path,
                                                                  const std::optional<std::string> &api_key,
                                                                  const std::optional<std::string> &database_id) {
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
	// Priority 3: DuckDB secret manager
	else {
		creds = GetFirestoreCredentialsFromSecret(context);
	}

	// Priority 4: GOOGLE_APPLICATION_CREDENTIALS environment variable
	if (!creds) {
		const char *env_creds = std::getenv("GOOGLE_APPLICATION_CREDENTIALS");
		if (env_creds && strlen(env_creds) > 0) {
			creds = FirestoreAuthManager::LoadServiceAccount(env_creds);
		}
	}

	// No credentials found
	if (!creds) {
		return nullptr;
	}

	// Apply database_id if explicitly provided (overrides secret value)
	if (database_id.has_value()) {
		creds->database_id = database_id.value();
	}

	return creds;
}

} // namespace duckdb
