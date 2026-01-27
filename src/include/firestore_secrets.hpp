#pragma once

#include "duckdb.hpp"
#include "firestore_auth.hpp"
#include <memory>

namespace duckdb {

class ExtensionLoader;

// Register the firestore secret type and providers
void RegisterFirestoreSecretType(ExtensionLoader &loader);

// Get Firestore credentials from the secret manager
std::shared_ptr<FirestoreCredentials> GetFirestoreCredentialsFromSecret(ClientContext &context,
                                                                        const std::string &secret_name = "");

// Try to get credentials from various sources (secret, parameters, env)
std::shared_ptr<FirestoreCredentials>
ResolveFirestoreCredentials(ClientContext &context, const std::optional<std::string> &project_id,
                            const std::optional<std::string> &credentials_path,
                            const std::optional<std::string> &api_key,
                            const std::optional<std::string> &database_id = std::nullopt);

} // namespace duckdb
