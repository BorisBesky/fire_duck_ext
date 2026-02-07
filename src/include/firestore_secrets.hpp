#pragma once

#include "duckdb.hpp"
#include "firestore_auth.hpp"
#include <memory>
#include <optional>

namespace duckdb {

class ExtensionLoader;

// Register the firestore secret type and providers
void RegisterFirestoreSecretType(ExtensionLoader &loader);

// Create the env-backed secret if GOOGLE_APPLICATION_CREDENTIALS is set
void EnsureFirestoreEnvSecret(DatabaseInstance &db);

// Get Firestore credentials from the secret manager
// If target_database is provided, only returns credentials that match that database
std::shared_ptr<FirestoreCredentials>
GetFirestoreCredentialsFromSecret(ClientContext &context, const std::string &secret_name = "",
                                  const std::optional<std::string> &target_database = std::nullopt);

// Try to get credentials from various sources (secret, parameters, env)
std::shared_ptr<FirestoreCredentials>
ResolveFirestoreCredentials(ClientContext &context, const std::optional<std::string> &project_id,
                            const std::optional<std::string> &credentials_path,
                            const std::optional<std::string> &api_key,
                            const std::optional<std::string> &database_id = std::nullopt);

// Clear all cached credentials (service account and API key)
void ClearFirestoreCredentialsCache();

// ============================================================================
// Session-scoped database connection management
// ============================================================================

// Get the currently connected database_id for this session (if any)
std::optional<std::string> GetConnectedDatabase(ClientContext &context);

// Set the connected database_id for this session
void SetConnectedDatabase(ClientContext &context, const std::string &database_id);

// Clear the connected database (resets to default behavior)
void ClearConnectedDatabase(ClientContext &context);

// Check if a secret's database value matches a target database
// Supports: exact match, array of databases, or '*' wildcard
bool DatabaseMatchesSecret(const Value &secret_db_value, const std::string &target_db);

} // namespace duckdb
