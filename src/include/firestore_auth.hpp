#pragma once

#include <string>
#include <memory>
#include <chrono>
#include <optional>

namespace duckdb {

class DatabaseInstance;

enum class FirestoreAuthType {
	SERVICE_ACCOUNT, // OAuth2 via signed JWT (admin; bypasses Security Rules; native only)
	API_KEY,         // unauthenticated; only the project is identified (request.auth == null)
	FIREBASE_USER    // Firebase Auth user ID token (browser-safe; respects Security Rules)
};

struct FirestoreCredentials {
	FirestoreAuthType type;
	std::string project_id;
	std::string database_id = "(default)"; // Database ID, defaults to "(default)"

	// For SERVICE_ACCOUNT
	std::string client_email;
	std::string private_key;
	std::string private_key_id;

	// For API_KEY (also used by FIREBASE_USER for the Firebase Auth endpoints)
	std::string api_key;

	// For FIREBASE_USER (Firebase Auth user sign-in)
	std::string email;
	std::string password;
	bool anonymous = false;
	std::string refresh_token;

	// Cached access token: OAuth2 token for SERVICE_ACCOUNT, ID token for FIREBASE_USER
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

	// Create credentials for Firebase Auth user sign-in (anonymous or email/password).
	// The api_key is the public Web API key, used for the Firebase Auth endpoints.
	static std::unique_ptr<FirestoreCredentials> CreateFirebaseUserCredentials(const std::string &project_id,
	                                                                           const std::string &api_key,
	                                                                           const std::string &email,
	                                                                           const std::string &password,
	                                                                           bool anonymous);

	// Get/refresh OAuth2 access token for service account.
	// `db` is used to reach DuckDB's HTTPUtil for the token HTTP calls under WASM.
	static std::string GetAccessToken(FirestoreCredentials &creds, DatabaseInstance &db);

	// Refresh/acquire the cached token if needed (service account OAuth2, or Firebase
	// user ID token). `db` is used for the HTTP calls (HTTPUtil on WASM).
	static void RefreshTokenIfNeeded(FirestoreCredentials &creds, DatabaseInstance &db);

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
