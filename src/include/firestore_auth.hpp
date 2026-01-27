#pragma once

#include <string>
#include <memory>
#include <chrono>
#include <optional>

namespace duckdb {

enum class FirestoreAuthType { SERVICE_ACCOUNT, API_KEY };

struct FirestoreCredentials {
	FirestoreAuthType type;
	std::string project_id;
	std::string database_id = "(default)"; // Database ID, defaults to "(default)"

	// For SERVICE_ACCOUNT
	std::string client_email;
	std::string private_key;
	std::string private_key_id;

	// For API_KEY
	std::string api_key;

	// Cached access token (for SERVICE_ACCOUNT)
	std::string access_token;
	std::chrono::system_clock::time_point token_expiry;

	bool IsTokenValid() const;
	std::string GetAuthHeader() const;
	std::string GetUrlSuffix() const;
};

class FirestoreAuthManager {
public:
	// Load credentials from service account JSON file
	static std::unique_ptr<FirestoreCredentials> LoadServiceAccount(const std::string &json_path);

	// Parse service account JSON content directly
	static std::unique_ptr<FirestoreCredentials> ParseServiceAccountJson(const std::string &json_content);

	// Create credentials from API key
	static std::unique_ptr<FirestoreCredentials> CreateApiKeyCredentials(const std::string &project_id,
	                                                                     const std::string &api_key);

	// Get/refresh OAuth2 access token for service account
	static std::string GetAccessToken(FirestoreCredentials &creds);

	// Refresh token if needed
	static void RefreshTokenIfNeeded(FirestoreCredentials &creds);

private:
	// Create JWT for service account authentication
	static std::string CreateJWT(const FirestoreCredentials &creds);

	// Exchange JWT for access token via Google OAuth2
	static std::string ExchangeJWTForToken(const std::string &jwt);

	// Sign data with RS256 using private key
	static std::string SignRS256(const std::string &data, const std::string &private_key);

	// Base64URL encode (no padding, URL-safe)
	static std::string Base64UrlEncode(const std::string &data);
	static std::string Base64UrlEncode(const unsigned char *data, size_t len);
};

} // namespace duckdb
