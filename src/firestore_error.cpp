#include "firestore_error.hpp"
#include <sstream>
#include <iomanip>

namespace duckdb {

// ============================================================================
// Error Code to String Conversion
// ============================================================================

const char* FirestoreErrorCodeToString(FirestoreErrorCode code) {
    switch (code) {
        case FirestoreErrorCode::SUCCESS: return "Success";

        // Auth errors
        case FirestoreErrorCode::AUTH_BASE: return "Authentication error";
        case FirestoreErrorCode::AUTH_CREDENTIALS_NULL: return "Credentials cannot be null";
        case FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_FILE: return "Cannot open service account file";
        case FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_PARSE: return "Cannot parse service account JSON";
        case FirestoreErrorCode::AUTH_SERVICE_ACCOUNT_FIELDS: return "Missing required fields in service account";
        case FirestoreErrorCode::AUTH_PRIVATE_KEY_INVALID: return "Cannot read private key";
        case FirestoreErrorCode::AUTH_JWT_CREATION_FAILED: return "JWT creation failed";
        case FirestoreErrorCode::AUTH_SIGNING_FAILED: return "RS256 signing failed";
        case FirestoreErrorCode::AUTH_TOKEN_EXCHANGE_FAILED: return "Token exchange failed";
        case FirestoreErrorCode::AUTH_TOKEN_PARSE_FAILED: return "Cannot parse token response";
        case FirestoreErrorCode::AUTH_TOKEN_MISSING: return "Token response missing access_token";
        case FirestoreErrorCode::AUTH_TOKEN_EXPIRED: return "Authentication token expired";
        case FirestoreErrorCode::AUTH_API_KEY_INVALID: return "API key rejected";
        case FirestoreErrorCode::AUTH_INVALID_TYPE: return "Invalid authentication type";

        // Permission errors
        case FirestoreErrorCode::PERMISSION_BASE: return "Permission error";
        case FirestoreErrorCode::PERMISSION_DENIED: return "Permission denied";
        case FirestoreErrorCode::PERMISSION_INSUFFICIENT: return "Insufficient permissions";
        case FirestoreErrorCode::PERMISSION_SECURITY_RULES: return "Blocked by security rules";

        // Not found errors
        case FirestoreErrorCode::NOT_FOUND_BASE: return "Not found";
        case FirestoreErrorCode::NOT_FOUND_DOCUMENT: return "Document not found";
        case FirestoreErrorCode::NOT_FOUND_COLLECTION: return "Collection not found";
        case FirestoreErrorCode::NOT_FOUND_PROJECT: return "Project not found";
        case FirestoreErrorCode::NOT_FOUND_DATABASE: return "Database not found";

        // Network errors
        case FirestoreErrorCode::NETWORK_BASE: return "Network error";
        case FirestoreErrorCode::NETWORK_CURL_INIT: return "CURL initialization failed";
        case FirestoreErrorCode::NETWORK_CURL_PERFORM: return "HTTP request failed";
        case FirestoreErrorCode::NETWORK_TIMEOUT: return "Request timed out";
        case FirestoreErrorCode::NETWORK_DNS_RESOLUTION: return "DNS resolution failed";
        case FirestoreErrorCode::NETWORK_CONNECTION_REFUSED: return "Connection refused";
        case FirestoreErrorCode::NETWORK_SSL_ERROR: return "SSL/TLS error";

        // Request errors
        case FirestoreErrorCode::REQUEST_BASE: return "Request error";
        case FirestoreErrorCode::REQUEST_INVALID_URL: return "Invalid URL";
        case FirestoreErrorCode::REQUEST_RESPONSE_PARSE: return "Cannot parse response";
        case FirestoreErrorCode::REQUEST_UNEXPECTED_FORMAT: return "Unexpected response format";
        case FirestoreErrorCode::REQUEST_RATE_LIMITED: return "Rate limited";
        case FirestoreErrorCode::REQUEST_QUOTA_EXCEEDED: return "Quota exceeded";
        case FirestoreErrorCode::REQUEST_SERVER_ERROR: return "Server error";

        // Config errors
        case FirestoreErrorCode::CONFIG_BASE: return "Configuration error";
        case FirestoreErrorCode::CONFIG_MISSING_PROJECT_ID: return "Missing project_id";
        case FirestoreErrorCode::CONFIG_MISSING_CREDENTIALS: return "Missing credentials";
        case FirestoreErrorCode::CONFIG_MISSING_API_KEY: return "Missing api_key";
        case FirestoreErrorCode::CONFIG_SECRET_INVALID: return "Invalid secret configuration";
        case FirestoreErrorCode::CONFIG_SECRET_AUTH_TYPE: return "Unknown auth_type";

        // Type errors
        case FirestoreErrorCode::TYPE_BASE: return "Type conversion error";
        case FirestoreErrorCode::TYPE_CONVERSION_FAILED: return "Type conversion failed";
        case FirestoreErrorCode::TYPE_TIMESTAMP_PARSE: return "Cannot parse timestamp";
        case FirestoreErrorCode::TYPE_INTEGER_OVERFLOW: return "Integer overflow";
        case FirestoreErrorCode::TYPE_DOUBLE_PARSE: return "Cannot parse double";
        case FirestoreErrorCode::TYPE_UNKNOWN_FIRESTORE_TYPE: return "Unknown Firestore type";
        case FirestoreErrorCode::TYPE_UNSUPPORTED_DUCKDB: return "Unsupported DuckDB type";

        // Write errors
        case FirestoreErrorCode::WRITE_BASE: return "Write operation error";
        case FirestoreErrorCode::WRITE_FIELD_NAME_INVALID: return "Invalid field name";
        case FirestoreErrorCode::WRITE_FIELD_VALUE_INVALID: return "Invalid field value";
        case FirestoreErrorCode::WRITE_DOC_ID_INVALID: return "Invalid document ID";
        case FirestoreErrorCode::WRITE_BATCH_EMPTY: return "Empty batch operation";
        case FirestoreErrorCode::WRITE_BATCH_TOO_LARGE: return "Batch too large";
        case FirestoreErrorCode::WRITE_BATCH_PARTIAL_FAILURE: return "Batch partially failed";
        case FirestoreErrorCode::WRITE_UPDATE_NO_FIELDS: return "No fields to update";
        case FirestoreErrorCode::WRITE_INSERT_FAILED: return "Insert failed";
        case FirestoreErrorCode::WRITE_UPDATE_FAILED: return "Update failed";
        case FirestoreErrorCode::WRITE_DELETE_FAILED: return "Delete failed";

        // Scan errors
        case FirestoreErrorCode::SCAN_BASE: return "Scan error";
        case FirestoreErrorCode::SCAN_COLLECTION_REQUIRED: return "Collection name required";
        case FirestoreErrorCode::SCAN_SCHEMA_INFERENCE: return "Schema inference failed";
        case FirestoreErrorCode::SCAN_INVALID_LIMIT: return "Invalid limit value";
        case FirestoreErrorCode::SCAN_INVALID_ORDER_BY: return "Invalid order_by field";

        // Internal errors
        case FirestoreErrorCode::INTERNAL_BASE: return "Internal error";
        case FirestoreErrorCode::INTERNAL_UNEXPECTED: return "Unexpected internal error";

        default: return "Unknown error";
    }
}

std::string FormatErrorCode(FirestoreErrorCode code) {
    std::ostringstream ss;
    ss << "FS_" << std::hex << std::uppercase << std::setfill('0') << std::setw(8)
       << static_cast<uint32_t>(code);
    return ss.str();
}

// ============================================================================
// FirestoreErrorContext Implementation
// ============================================================================

std::string FirestoreErrorContext::ToString() const {
    std::ostringstream ss;
    bool first = true;

    auto append = [&](const std::string& key, const std::string& value) {
        if (!first) ss << ", ";
        ss << key << "=" << value;
        first = false;
    };

    ss << "{";
    if (operation) append("operation", *operation);
    if (collection) append("collection", *collection);
    if (document_id) append("document_id", *document_id);
    if (http_method) append("method", *http_method);
    if (http_status_code) append("status", std::to_string(*http_status_code));
    if (url) {
        // Truncate URL for readability
        std::string truncated_url = url->length() > 100 ? url->substr(0, 100) + "..." : *url;
        append("url", truncated_url);
    }
    if (project_id) append("project", *project_id);
    if (database_id) append("database", *database_id);
    if (batch_index) append("batch_index", std::to_string(*batch_index));
    ss << "}";

    return ss.str();
}

// ============================================================================
// FirestoreError Implementation
// ============================================================================

FirestoreError::FirestoreError(const std::string& message)
    : code_(FirestoreErrorCode::INTERNAL_UNEXPECTED)
    , message_(message)
    , has_context_(false) {
    build_what_cache();
}

FirestoreError::FirestoreError(FirestoreErrorCode code, const std::string& message)
    : code_(code)
    , message_(message)
    , has_context_(false) {
    build_what_cache();
}

FirestoreError::FirestoreError(FirestoreErrorCode code, const std::string& message,
                               const FirestoreErrorContext& context)
    : code_(code)
    , message_(message)
    , context_(context)
    , has_context_(true) {
    build_what_cache();
}

const char* FirestoreError::what() const noexcept {
    return what_cache_.c_str();
}

std::string FirestoreError::formatted_message() const {
    std::ostringstream ss;
    ss << "[" << FormatErrorCode(code_) << "] " << message_;
    return ss.str();
}

void FirestoreError::build_what_cache() const {
    std::ostringstream ss;
    ss << "[" << FormatErrorCode(code_) << "] " << message_;
    if (has_context_) {
        ss << " " << context_.ToString();
    }
    what_cache_ = ss.str();
}

} // namespace duckdb
