#include "firestore_auth.hpp"
#include "firestore_error.hpp"
#include "firestore_logger.hpp"
#include <fstream>
#include <sstream>
#include <ctime>
#ifndef __EMSCRIPTEN__
// Service-account auth (JWT signing + OAuth token exchange) relies on OpenSSL and
// raw sockets, neither of which is available on DuckDB-WASM. These paths compile
// only on native builds; on WASM the corresponding functions throw (see below).
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/err.h>
#else
// On WASM, Firebase user auth (sign-in + token refresh) goes through DuckDB's
// HTTPUtil — pure HTTP, no OpenSSL. (Service-account signing is still unsupported.)
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/helper.hpp"
#endif
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

std::unique_ptr<FirestoreCredentials>
FirestoreAuthManager::CreateFirebaseUserCredentials(const std::string &project_id, const std::string &api_key,
                                                    const std::string &email, const std::string &password,
                                                    bool anonymous) {
	FS_LOG_DEBUG("Creating Firebase user credentials for project: " + project_id +
	             (anonymous ? " (anonymous)" : " (email/password)"));

	auto creds = std::make_unique<FirestoreCredentials>();
	creds->type = FirestoreAuthType::FIREBASE_USER;
	creds->project_id = project_id;
	creds->api_key = api_key;
	creds->email = email;
	creds->password = password;
	creds->anonymous = anonymous;
	return creds;
}

std::string FirestoreAuthManager::Base64UrlEncode(const std::string &data) {
	return Base64UrlEncode(reinterpret_cast<const unsigned char *>(data.data()), data.size());
}

std::string FirestoreAuthManager::Base64UrlEncode(const unsigned char *data, size_t len) {
#ifdef __EMSCRIPTEN__
	(void)data;
	(void)len;
	throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
	                         "Service account authentication is not supported on DuckDB-WASM. "
	                         "Use API key authentication instead.");
#else
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
#endif
}

std::string FirestoreAuthManager::SignRS256(const std::string &data, const std::string &private_key) {
#ifdef __EMSCRIPTEN__
	(void)data;
	(void)private_key;
	throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
	                         "Service account authentication is not supported on DuckDB-WASM. "
	                         "Use API key authentication instead.");
#else
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
#endif
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
#ifdef __EMSCRIPTEN__
	(void)jwt;
	throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
	                         "Service account authentication is not supported on DuckDB-WASM. "
	                         "Use API key authentication instead.");
#else
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
#endif
}

namespace {

// Extract a human-readable message from a Firebase Auth error response body.
std::string ExtractFirebaseAuthError(const std::string &body) {
	try {
		auto j = json::parse(body);
		if (j.contains("error") && j["error"].is_object() && j["error"].contains("message")) {
			return j["error"]["message"].get<std::string>();
		}
	} catch (...) {
		// fall through to raw body
	}
	return body.substr(0, 200);
}

struct AuthHttpResponse {
	int status;
	std::string body;
};

// Single HTTP POST used by the Firebase Auth endpoints. Dual-path: HTTPUtil on WASM
// (browser fetch / Node), httplib on native. No OpenSSL on either path.
AuthHttpResponse FirebaseAuthPost(DatabaseInstance &db, const std::string &url, const std::string &body,
                                  const std::string &content_type) {
#ifdef __EMSCRIPTEN__
	auto &http = HTTPUtil::Get(db);
	auto params = http.InitializeParameters(db, url);
	HTTPHeaders headers(db);
	headers.Insert("Content-Type", content_type);
	PostRequestInfo req(url, headers, *params, const_data_ptr_cast(body.c_str()), body.size());
	req.try_request = true;
	auto res = http.Request(req);
	if (res->HasRequestError()) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "Firebase Auth HTTP request failed: " + res->GetRequestError());
	}
	std::string out = req.buffer_out.empty() ? res->body : req.buffer_out;
	return {static_cast<int>(res->status), out};
#else
	(void)db;
	// Split "scheme://host[:port]/path?query" into the httplib base + path.
	auto scheme_end = url.find("://");
	auto host_start = (scheme_end == std::string::npos) ? 0 : scheme_end + 3;
	auto path_start = url.find('/', host_start);
	std::string scheme_host = url.substr(0, path_start);
	std::string path = url.substr(path_start);

	httplib::Client cli(scheme_host);
	cli.set_connection_timeout(30);
	cli.set_read_timeout(30);
	auto res = cli.Post(path, body, content_type);
	if (!res) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "Firebase Auth HTTP request failed: " + httplib::to_string(res.error()));
	}
	return {res->status, res->body};
#endif
}

// Sign in (anonymous or email/password) via the Firebase Auth REST API and store the
// resulting ID + refresh tokens on the credentials.
void FirebaseSignIn(FirestoreCredentials &creds, DatabaseInstance &db) {
	std::string url;
	json req_body;
	if (creds.anonymous) {
		FS_LOG_DEBUG("Firebase anonymous sign-in");
		url = "https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=" + creds.api_key;
		req_body["returnSecureToken"] = true;
	} else if (!creds.email.empty()) {
		FS_LOG_DEBUG("Firebase email/password sign-in for: " + creds.email);
		url = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=" + creds.api_key;
		req_body["email"] = creds.email;
		req_body["password"] = creds.password;
		req_body["returnSecureToken"] = true;
	} else {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
		                         "Firebase user credentials require email/password or the anonymous flag");
	}

	auto resp = FirebaseAuthPost(db, url, req_body.dump(), "application/json");
	if (resp.status != 200) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "Firebase sign-in failed (HTTP " + std::to_string(resp.status) +
		                             "): " + ExtractFirebaseAuthError(resp.body));
	}

	try {
		auto j = json::parse(resp.body);
		creds.access_token = j.at("idToken").get<std::string>();
		creds.refresh_token = j.value("refreshToken", std::string());
		int expires_in = std::stoi(j.value("expiresIn", std::string("3600")));
		creds.token_expiry = std::chrono::system_clock::now() + std::chrono::seconds(expires_in);
	} catch (const std::exception &e) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_PARSE_FAILED,
		                         "Failed to parse Firebase sign-in response: " + std::string(e.what()));
	}
	FS_LOG_DEBUG("Firebase sign-in succeeded");
}

// Exchange a refresh token for a fresh ID token via securetoken.googleapis.com.
void FirebaseRefresh(FirestoreCredentials &creds, DatabaseInstance &db) {
	FS_LOG_DEBUG("Refreshing Firebase ID token");
	std::string url = "https://securetoken.googleapis.com/v1/token?key=" + creds.api_key;
	std::string body = "grant_type=refresh_token&refresh_token=" + creds.refresh_token;

	auto resp = FirebaseAuthPost(db, url, body, "application/x-www-form-urlencoded");
	if (resp.status != 200) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED,
		                         "Firebase token refresh failed (HTTP " + std::to_string(resp.status) +
		                             "): " + ExtractFirebaseAuthError(resp.body));
	}

	try {
		// The securetoken endpoint returns snake_case fields.
		auto j = json::parse(resp.body);
		creds.access_token = j.at("id_token").get<std::string>();
		creds.refresh_token = j.value("refresh_token", creds.refresh_token);
		int expires_in = std::stoi(j.value("expires_in", std::string("3600")));
		creds.token_expiry = std::chrono::system_clock::now() + std::chrono::seconds(expires_in);
	} catch (const std::exception &e) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_PARSE_FAILED,
		                         "Failed to parse Firebase refresh response: " + std::string(e.what()));
	}
	FS_LOG_DEBUG("Firebase ID token refreshed");
}

} // namespace

std::string FirestoreAuthManager::GetAccessToken(FirestoreCredentials &creds, DatabaseInstance &db) {
	if (creds.type != FirestoreAuthType::SERVICE_ACCOUNT) {
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_INVALID_TYPE,
		                         "GetAccessToken only works with service account credentials");
	}

	RefreshTokenIfNeeded(creds, db);
	return creds.access_token;
}

void FirestoreAuthManager::RefreshTokenIfNeeded(FirestoreCredentials &creds, DatabaseInstance &db) {
	if (creds.type == FirestoreAuthType::API_KEY) {
		return; // API keys don't expire and carry no token
	}

	if (creds.IsTokenValid()) {
		return; // Token still valid
	}

	if (creds.type == FirestoreAuthType::SERVICE_ACCOUNT) {
		FS_LOG_DEBUG("Refreshing access token");

		// Create JWT and exchange for access token
		std::string jwt = CreateJWT(creds);
		creds.access_token = ExchangeJWTForToken(jwt);
		creds.token_expiry = std::chrono::system_clock::now() + std::chrono::hours(1);

		FS_LOG_DEBUG("Access token refreshed successfully");
		return;
	}

	if (creds.type == FirestoreAuthType::FIREBASE_USER) {
		// Refresh with the stored refresh token when we have one; otherwise sign in.
		// If the refresh fails (e.g. revoked token), fall back to a fresh sign-in.
		if (!creds.refresh_token.empty()) {
			try {
				FirebaseRefresh(creds, db);
				return;
			} catch (const std::exception &e) {
				FS_LOG_DEBUG("Firebase token refresh failed, re-signing in: " + std::string(e.what()));
				creds.refresh_token.clear();
			}
		}
		FirebaseSignIn(creds, db);
		return;
	}
}

} // namespace duckdb
