#include "firestore_auth.hpp"
#include "firestore_error.hpp"
#include "firestore_logger.hpp"
#include <fstream>
#include <sstream>
#include <ctime>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#include <nlohmann/json.hpp>

namespace duckdb {

using json = nlohmann::json;

// Token validity buffer (refresh 5 minutes before expiry)
static const int TOKEN_REFRESH_BUFFER_SECONDS = 300;

// Google OAuth2 token endpoint
static const char *GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";

// Firestore scope
static const char *FIRESTORE_SCOPE = "https://www.googleapis.com/auth/datastore";

bool FirestoreCredentials::IsTokenValid() const {
	if (type == FirestoreAuthType::API_KEY) {
		return true; // API keys don't expire
	}
	if (access_token.empty()) {
		return false;
	}
	auto now = std::chrono::system_clock::now();
	return now < (token_expiry - std::chrono::seconds(TOKEN_REFRESH_BUFFER_SECONDS));
}

std::string FirestoreCredentials::GetAuthHeader() const {
	if (type == FirestoreAuthType::API_KEY) {
		return ""; // API key goes in URL, not header
	}
	return "Bearer " + access_token;
}

std::string FirestoreCredentials::GetUrlSuffix() const {
	if (type == FirestoreAuthType::API_KEY) {
		return "?key=" + api_key;
	}
	return "";
}

std::unique_ptr<FirestoreCredentials> FirestoreAuthManager::LoadServiceAccount(const std::string &json_path) {
	FS_LOG_DEBUG("Loading service account from: " + json_path);

	std::ifstream file(json_path);
	if (!file.is_open()) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_FILE,
		                         "Failed to open service account file: " + json_path);
	}

	std::stringstream buffer;
	buffer << file.rdbuf();
	return ParseServiceAccountJson(buffer.str());
}

std::unique_ptr<FirestoreCredentials> FirestoreAuthManager::ParseServiceAccountJson(const std::string &json_content) {
	auto creds = std::make_unique<FirestoreCredentials>();
	creds->type = FirestoreAuthType::SERVICE_ACCOUNT;

	try {
		auto j = json::parse(json_content);

		if (!j.contains("project_id") || !j.contains("private_key") || !j.contains("client_email")) {
			throw FirestoreAuthError(
			    FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_FIELDS,
			    "Service account JSON missing required fields (project_id, private_key, client_email)");
		}

		creds->project_id = j["project_id"].get<std::string>();
		creds->private_key = j["private_key"].get<std::string>();
		creds->client_email = j["client_email"].get<std::string>();

		if (j.contains("private_key_id")) {
			creds->private_key_id = j["private_key_id"].get<std::string>();
		}

		FS_LOG_DEBUG("Loaded service account for project: " + creds->project_id);
	} catch (const FirestoreAuthError &) {
		throw; // Re-throw our own errors
	} catch (const json::exception &e) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_PARSE,
		                         "Failed to parse service account JSON: " + std::string(e.what()));
	}

	return creds;
}

std::unique_ptr<FirestoreCredentials> FirestoreAuthManager::CreateApiKeyCredentials(const std::string &project_id,
                                                                                    const std::string &api_key) {
	FS_LOG_DEBUG("Creating API key credentials for project: " + project_id);

	auto creds = std::make_unique<FirestoreCredentials>();
	creds->type = FirestoreAuthType::API_KEY;
	creds->project_id = project_id;
	creds->api_key = api_key;
	return creds;
}

std::string FirestoreAuthManager::Base64UrlEncode(const std::string &data) {
	return Base64UrlEncode(reinterpret_cast<const unsigned char *>(data.data()), data.size());
}

std::string FirestoreAuthManager::Base64UrlEncode(const unsigned char *data, size_t len) {
	BIO *bio, *b64;
	BUF_MEM *bufferPtr;

	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new(BIO_s_mem());
	bio = BIO_push(b64, bio);

	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(bio, data, len);
	BIO_flush(bio);
	BIO_get_mem_ptr(bio, &bufferPtr);

	std::string result(bufferPtr->data, bufferPtr->length);
	BIO_free_all(bio);

	// Convert to URL-safe base64
	for (auto &c : result) {
		if (c == '+')
			c = '-';
		else if (c == '/')
			c = '_';
	}

	// Remove padding
	while (!result.empty() && result.back() == '=') {
		result.pop_back();
	}

	return result;
}

std::string FirestoreAuthManager::SignRS256(const std::string &data, const std::string &private_key) {
	// Create BIO from private key string
	BIO *bio = BIO_new_mem_buf(private_key.data(), private_key.size());
	if (!bio) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_PRIVATE_KEY_INVALID, "Failed to create BIO for private key");
	}

	// Read private key
	EVP_PKEY *pkey = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
	BIO_free(bio);

	if (!pkey) {
		unsigned long err = ERR_get_error();
		char err_buf[256];
		ERR_error_string_n(err, err_buf, sizeof(err_buf));
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_PRIVATE_KEY_INVALID,
		                         "Failed to read private key: " + std::string(err_buf));
	}

	// Create signing context
	EVP_MD_CTX *ctx = EVP_MD_CTX_new();
	if (!ctx) {
		EVP_PKEY_free(pkey);
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SIGNING_FAILED, "Failed to create signing context");
	}

	// Initialize signing
	if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, pkey) != 1) {
		EVP_MD_CTX_free(ctx);
		EVP_PKEY_free(pkey);
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SIGNING_FAILED, "Failed to initialize signing");
	}

	// Sign
	if (EVP_DigestSignUpdate(ctx, data.data(), data.size()) != 1) {
		EVP_MD_CTX_free(ctx);
		EVP_PKEY_free(pkey);
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SIGNING_FAILED, "Failed to update signing");
	}

	// Get signature size
	size_t sig_len;
	if (EVP_DigestSignFinal(ctx, nullptr, &sig_len) != 1) {
		EVP_MD_CTX_free(ctx);
		EVP_PKEY_free(pkey);
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SIGNING_FAILED, "Failed to get signature size");
	}

	// Get signature
	std::vector<unsigned char> sig(sig_len);
	if (EVP_DigestSignFinal(ctx, sig.data(), &sig_len) != 1) {
		EVP_MD_CTX_free(ctx);
		EVP_PKEY_free(pkey);
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_SIGNING_FAILED, "Failed to sign data");
	}

	EVP_MD_CTX_free(ctx);
	EVP_PKEY_free(pkey);

	return Base64UrlEncode(sig.data(), sig_len);
}

std::string FirestoreAuthManager::CreateJWT(const FirestoreCredentials &creds) {
	FS_LOG_DEBUG("Creating JWT for: " + creds.client_email);

	auto now = std::chrono::system_clock::now();
	auto now_secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
	auto exp_secs = now_secs + 3600; // 1 hour validity

	// JWT Header
	json header = {{"alg", "RS256"}, {"typ", "JWT"}};

	// JWT Payload
	json payload = {{"iss", creds.client_email},
	                {"scope", FIRESTORE_SCOPE},
	                {"aud", GOOGLE_TOKEN_URL},
	                {"iat", now_secs},
	                {"exp", exp_secs}};

	std::string header_b64 = Base64UrlEncode(header.dump());
	std::string payload_b64 = Base64UrlEncode(payload.dump());
	std::string unsigned_token = header_b64 + "." + payload_b64;

	std::string signature = SignRS256(unsigned_token, creds.private_key);

	return unsigned_token + "." + signature;
}

std::string FirestoreAuthManager::ExchangeJWTForToken(const std::string &jwt) {
	FS_LOG_DEBUG("Exchanging JWT for access token");

	std::string post_data = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + jwt;

	httplib::Client cli("https://oauth2.googleapis.com");
	cli.set_connection_timeout(30);
	cli.set_read_timeout(30);

	auto res = cli.Post("/token", post_data, "application/x-www-form-urlencoded");

	if (!res) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "HTTP request failed: " + httplib::to_string(res.error()));
	}

	if (res->status != 200) {
		FS_LOG_ERROR("Token exchange failed with HTTP " + std::to_string(res->status));
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "Token exchange failed with HTTP " + std::to_string(res->status) + ": " + res->body);
	}

	try {
		auto j = json::parse(res->body);
		if (!j.contains("access_token")) {
			throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_MISSING, "Token response missing access_token");
		}
		FS_LOG_DEBUG("Successfully obtained access token");
		return j["access_token"].get<std::string>();
	} catch (const FirestoreAuthError &) {
		throw; // Re-throw our own errors
	} catch (const json::exception &e) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_PARSE_FAILED,
		                         "Failed to parse token response: " + std::string(e.what()));
	}
}

std::string FirestoreAuthManager::GetAccessToken(FirestoreCredentials &creds) {
	if (creds.type != FirestoreAuthType::SERVICE_ACCOUNT) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
		                         "GetAccessToken only works with service account credentials");
	}

	RefreshTokenIfNeeded(creds);
	return creds.access_token;
}

void FirestoreAuthManager::RefreshTokenIfNeeded(FirestoreCredentials &creds) {
	if (creds.type != FirestoreAuthType::SERVICE_ACCOUNT) {
		return; // API keys don't need refresh
	}

	if (creds.IsTokenValid()) {
		return; // Token still valid
	}

	FS_LOG_DEBUG("Refreshing access token");

	// Create JWT and exchange for access token
	std::string jwt = CreateJWT(creds);
	creds.access_token = ExchangeJWTForToken(jwt);
	creds.token_expiry = std::chrono::system_clock::now() + std::chrono::hours(1);

	FS_LOG_DEBUG("Access token refreshed successfully");
}

} // namespace duckdb
