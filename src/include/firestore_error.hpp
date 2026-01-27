#pragma once

#include <cstdint>
#include <string>
#include <optional>
#include <exception>
#include <vector>
#include <chrono>

namespace duckdb {

// ============================================================================
// Error Codes
// ============================================================================
// Error codes are 32-bit integers with structure:
// Bits 24-31: Category (0-255)
// Bits 16-23: Subcategory (0-255)
// Bits 0-15:  Specific error (0-65535)
//
// This allows stable error codes across versions - new errors
// can be added without changing existing codes.

enum class FirestoreErrorCode : uint32_t {
    // ========== SUCCESS (Category 0x00) ==========
    SUCCESS = 0x00000000,

    // ========== AUTHENTICATION ERRORS (Category 0x01) ==========
    AUTH_BASE                   = 0x01000000,
    AUTH_CREDENTIALS_NULL       = 0x01010001,  // Credentials object is null
    AUTH_SERVICE_ACCOUNT_FILE   = 0x01010002,  // Cannot open service account file
    AUTH_SERVICE_ACCOUNT_PARSE  = 0x01010003,  // Cannot parse service account JSON
    AUTH_SERVICE_ACCOUNT_FIELDS = 0x01010004,  // Missing required fields in SA JSON
    AUTH_PRIVATE_KEY_INVALID    = 0x01020001,  // Cannot read/parse private key
    AUTH_JWT_CREATION_FAILED    = 0x01020002,  // JWT creation failed
    AUTH_SIGNING_FAILED         = 0x01020003,  // RS256 signing failed
    AUTH_TOKEN_EXCHANGE_FAILED  = 0x01030001,  // OAuth2 token exchange failed
    AUTH_TOKEN_PARSE_FAILED     = 0x01030002,  // Cannot parse token response
    AUTH_TOKEN_MISSING          = 0x01030003,  // Token response missing access_token
    AUTH_TOKEN_EXPIRED          = 0x01030004,  // Token has expired (401 response)
    AUTH_API_KEY_INVALID        = 0x01040001,  // API key rejected by Firestore
    AUTH_INVALID_TYPE           = 0x01040002,  // Invalid auth type

    // ========== PERMISSION ERRORS (Category 0x02) ==========
    PERMISSION_BASE             = 0x02000000,
    PERMISSION_DENIED           = 0x02010001,  // 403 Forbidden
    PERMISSION_INSUFFICIENT     = 0x02010002,  // Insufficient permissions for operation
    PERMISSION_SECURITY_RULES   = 0x02010003,  // Blocked by Firestore security rules

    // ========== NOT FOUND ERRORS (Category 0x03) ==========
    NOT_FOUND_BASE              = 0x03000000,
    NOT_FOUND_DOCUMENT          = 0x03010001,  // Document does not exist
    NOT_FOUND_COLLECTION        = 0x03010002,  // Collection does not exist
    NOT_FOUND_PROJECT           = 0x03010003,  // Project not found
    NOT_FOUND_DATABASE          = 0x03010004,  // Database not found

    // ========== NETWORK ERRORS (Category 0x04) ==========
    NETWORK_BASE                = 0x04000000,
    NETWORK_CURL_INIT           = 0x04010001,  // CURL initialization failed
    NETWORK_CURL_PERFORM        = 0x04010002,  // CURL request failed
    NETWORK_TIMEOUT             = 0x04010003,  // Request timed out
    NETWORK_DNS_RESOLUTION      = 0x04010004,  // DNS resolution failed
    NETWORK_CONNECTION_REFUSED  = 0x04010005,  // Connection refused
    NETWORK_SSL_ERROR           = 0x04020001,  // SSL/TLS error

    // ========== REQUEST/RESPONSE ERRORS (Category 0x05) ==========
    REQUEST_BASE                = 0x05000000,
    REQUEST_INVALID_URL         = 0x05010001,  // Malformed URL
    REQUEST_RESPONSE_PARSE      = 0x05020001,  // Cannot parse JSON response
    REQUEST_UNEXPECTED_FORMAT   = 0x05020002,  // Response format unexpected
    REQUEST_RATE_LIMITED        = 0x05030001,  // 429 Too Many Requests
    REQUEST_QUOTA_EXCEEDED      = 0x05030002,  // Quota exceeded
    REQUEST_SERVER_ERROR        = 0x05040001,  // 5xx server error (transient)

    // ========== CONFIGURATION ERRORS (Category 0x06) ==========
    CONFIG_BASE                 = 0x06000000,
    CONFIG_MISSING_PROJECT_ID   = 0x06010001,  // project_id required
    CONFIG_MISSING_CREDENTIALS  = 0x06010002,  // No credentials available
    CONFIG_MISSING_API_KEY      = 0x06010003,  // api_key required with project_id
    CONFIG_SECRET_INVALID       = 0x06020001,  // Secret missing required fields
    CONFIG_SECRET_AUTH_TYPE     = 0x06020002,  // Unknown auth_type in secret

    // ========== TYPE CONVERSION ERRORS (Category 0x07) ==========
    TYPE_BASE                   = 0x07000000,
    TYPE_CONVERSION_FAILED      = 0x07010001,  // Generic conversion failure
    TYPE_TIMESTAMP_PARSE        = 0x07010002,  // Cannot parse timestamp string
    TYPE_INTEGER_OVERFLOW       = 0x07010003,  // Integer value out of range
    TYPE_DOUBLE_PARSE           = 0x07010004,  // Cannot parse double
    TYPE_UNKNOWN_FIRESTORE_TYPE = 0x07020001,  // Unknown Firestore type
    TYPE_UNSUPPORTED_DUCKDB     = 0x07020002,  // DuckDB type not supported

    // ========== WRITE OPERATION ERRORS (Category 0x08) ==========
    WRITE_BASE                  = 0x08000000,
    WRITE_FIELD_NAME_INVALID    = 0x08010001,  // Field name must be string
    WRITE_FIELD_VALUE_INVALID   = 0x08010002,  // Field value type not supported
    WRITE_DOC_ID_INVALID        = 0x08010003,  // Document ID format invalid
    WRITE_BATCH_EMPTY           = 0x08020001,  // Empty batch operation
    WRITE_BATCH_TOO_LARGE       = 0x08020002,  // Batch exceeds 500 document limit
    WRITE_BATCH_PARTIAL_FAILURE = 0x08020003,  // Some documents in batch failed
    WRITE_UPDATE_NO_FIELDS      = 0x08030001,  // Update with no fields specified
    WRITE_INSERT_FAILED         = 0x08040001,  // Insert operation failed
    WRITE_UPDATE_FAILED         = 0x08040002,  // Update operation failed
    WRITE_DELETE_FAILED         = 0x08040003,  // Delete operation failed

    // ========== QUERY/SCAN ERRORS (Category 0x09) ==========
    SCAN_BASE                   = 0x09000000,
    SCAN_COLLECTION_REQUIRED    = 0x09010001,  // Collection name required
    SCAN_SCHEMA_INFERENCE       = 0x09010002,  // Schema inference failed
    SCAN_INVALID_LIMIT          = 0x09010003,  // Invalid limit value
    SCAN_INVALID_ORDER_BY       = 0x09010004,  // Invalid order_by field

    // ========== INDEX/PUSHDOWN ERRORS (Category 0x0A) ==========
    INDEX_BASE                  = 0x0A000000,
    INDEX_FETCH_FAILED          = 0x0A010001,  // Failed to fetch index metadata
    INDEX_PARSE_FAILED          = 0x0A010002,  // Failed to parse index response
    INDEX_ADMIN_API_UNAVAILABLE = 0x0A010003,  // Admin API not available (emulator)
    INDEX_QUERY_REJECTED        = 0x0A020001,  // Firestore rejected the filtered query

    // ========== INTERNAL ERRORS (Category 0xFF) ==========
    INTERNAL_BASE               = 0xFF000000,
    INTERNAL_UNEXPECTED         = 0xFF000001,  // Unexpected internal error
};

// Category extraction helpers
constexpr uint8_t GetErrorCategory(FirestoreErrorCode code) {
    return static_cast<uint8_t>((static_cast<uint32_t>(code) >> 24) & 0xFF);
}

constexpr bool IsAuthError(FirestoreErrorCode code) {
    return GetErrorCategory(code) == 0x01;
}

constexpr bool IsPermissionError(FirestoreErrorCode code) {
    return GetErrorCategory(code) == 0x02;
}

constexpr bool IsNotFoundError(FirestoreErrorCode code) {
    return GetErrorCategory(code) == 0x03;
}

constexpr bool IsNetworkError(FirestoreErrorCode code) {
    return GetErrorCategory(code) == 0x04;
}

constexpr bool IsTransientError(FirestoreErrorCode code) {
    uint32_t c = static_cast<uint32_t>(code);
    return c == static_cast<uint32_t>(FirestoreErrorCode::NETWORK_TIMEOUT) ||
           c == static_cast<uint32_t>(FirestoreErrorCode::NETWORK_CONNECTION_REFUSED) ||
           c == static_cast<uint32_t>(FirestoreErrorCode::REQUEST_RATE_LIMITED) ||
           c == static_cast<uint32_t>(FirestoreErrorCode::REQUEST_SERVER_ERROR);
}

// Convert error code to human-readable string
const char* FirestoreErrorCodeToString(FirestoreErrorCode code);

// Format error code as hex string (e.g., "FS_01010002")
std::string FormatErrorCode(FirestoreErrorCode code);

// ============================================================================
// Error Context
// ============================================================================
// Structured error context for detailed diagnostics

struct FirestoreErrorContext {
    // Request context
    std::optional<std::string> http_method;
    std::optional<std::string> url;
    std::optional<int> http_status_code;

    // Document/collection context
    std::optional<std::string> collection;
    std::optional<std::string> document_id;
    std::optional<std::string> project_id;
    std::optional<std::string> database_id;

    // Operation context
    std::optional<std::string> operation;  // "scan", "insert", "update", "delete", etc.
    std::optional<size_t> batch_index;     // Index in batch operation

    // Raw response (truncated for large responses)
    std::optional<std::string> response_body;  // First 1KB of response

    FirestoreErrorContext() = default;

    // Builder pattern for fluent construction
    FirestoreErrorContext& withMethod(const std::string& m) { http_method = m; return *this; }
    FirestoreErrorContext& withUrl(const std::string& u) { url = u; return *this; }
    FirestoreErrorContext& withStatus(int s) { http_status_code = s; return *this; }
    FirestoreErrorContext& withCollection(const std::string& c) { collection = c; return *this; }
    FirestoreErrorContext& withDocument(const std::string& d) { document_id = d; return *this; }
    FirestoreErrorContext& withProject(const std::string& p) { project_id = p; return *this; }
    FirestoreErrorContext& withDatabase(const std::string& d) { database_id = d; return *this; }
    FirestoreErrorContext& withOperation(const std::string& o) { operation = o; return *this; }
    FirestoreErrorContext& withBatchIndex(size_t i) { batch_index = i; return *this; }
    FirestoreErrorContext& withResponseBody(const std::string& b) {
        response_body = b.substr(0, 1024);  // Truncate to 1KB
        return *this;
    }

    // Serialize to string for logging
    std::string ToString() const;
};

// ============================================================================
// Exception Classes
// ============================================================================

// Base exception class with error code and context
class FirestoreError : public std::exception {
public:
    explicit FirestoreError(const std::string& message);
    FirestoreError(FirestoreErrorCode code, const std::string& message);
    FirestoreError(FirestoreErrorCode code, const std::string& message,
                   const FirestoreErrorContext& context);

    virtual ~FirestoreError() = default;

    // std::exception interface
    const char* what() const noexcept override;

    // Error code access
    FirestoreErrorCode code() const noexcept { return code_; }
    uint32_t code_value() const noexcept { return static_cast<uint32_t>(code_); }

    // Context access
    const FirestoreErrorContext& context() const noexcept { return context_; }
    bool has_context() const noexcept { return has_context_; }

    // Raw message (without code prefix)
    const std::string& message() const noexcept { return message_; }

    // Formatted message with code prefix
    std::string formatted_message() const;

protected:
    FirestoreErrorCode code_;
    std::string message_;
    FirestoreErrorContext context_;
    bool has_context_;

    mutable std::string what_cache_;  // Cached what() result

    void build_what_cache() const;
};

// Specific exception types (backward compatible with existing catch blocks)
class FirestoreAuthError : public FirestoreError {
public:
    explicit FirestoreAuthError(const std::string& message)
        : FirestoreError(FirestoreErrorCode::AUTH_TOKEN_EXPIRED, message) {}
    FirestoreAuthError(FirestoreErrorCode code, const std::string& message)
        : FirestoreError(code, message) {}
    FirestoreAuthError(FirestoreErrorCode code, const std::string& message,
                       const FirestoreErrorContext& ctx)
        : FirestoreError(code, message, ctx) {}
};

class FirestorePermissionError : public FirestoreError {
public:
    explicit FirestorePermissionError(const std::string& message)
        : FirestoreError(FirestoreErrorCode::PERMISSION_DENIED, message) {}
    FirestorePermissionError(FirestoreErrorCode code, const std::string& message)
        : FirestoreError(code, message) {}
    FirestorePermissionError(FirestoreErrorCode code, const std::string& message,
                             const FirestoreErrorContext& ctx)
        : FirestoreError(code, message, ctx) {}
};

class FirestoreNotFoundError : public FirestoreError {
public:
    explicit FirestoreNotFoundError(const std::string& message)
        : FirestoreError(FirestoreErrorCode::NOT_FOUND_DOCUMENT, message) {}
    FirestoreNotFoundError(FirestoreErrorCode code, const std::string& message)
        : FirestoreError(code, message) {}
    FirestoreNotFoundError(FirestoreErrorCode code, const std::string& message,
                           const FirestoreErrorContext& ctx)
        : FirestoreError(code, message, ctx) {}
};

class FirestoreNetworkError : public FirestoreError {
public:
    explicit FirestoreNetworkError(const std::string& message)
        : FirestoreError(FirestoreErrorCode::NETWORK_CURL_PERFORM, message) {}
    FirestoreNetworkError(FirestoreErrorCode code, const std::string& message)
        : FirestoreError(code, message) {}
    FirestoreNetworkError(FirestoreErrorCode code, const std::string& message,
                          const FirestoreErrorContext& ctx)
        : FirestoreError(code, message, ctx) {}
};

class FirestoreTypeError : public FirestoreError {
public:
    explicit FirestoreTypeError(const std::string& message)
        : FirestoreError(FirestoreErrorCode::TYPE_CONVERSION_FAILED, message) {}
    FirestoreTypeError(FirestoreErrorCode code, const std::string& message)
        : FirestoreError(code, message) {}
    FirestoreTypeError(FirestoreErrorCode code, const std::string& message,
                       const FirestoreErrorContext& ctx)
        : FirestoreError(code, message, ctx) {}
};

// ============================================================================
// Batch Operation Results
// ============================================================================

struct BatchOperationResult {
    size_t total_requested = 0;
    size_t succeeded = 0;
    size_t failed = 0;

    struct FailedItem {
        size_t index;
        std::string document_id;
        FirestoreErrorCode error_code;
        std::string error_message;
    };
    std::vector<FailedItem> failures;

    bool has_failures() const { return !failures.empty(); }
    bool all_failed() const { return failed == total_requested && total_requested > 0; }
    bool all_succeeded() const { return succeeded == total_requested; }

    void add_failure(size_t idx, const std::string& doc_id,
                     FirestoreErrorCode code, const std::string& msg) {
        failures.push_back({idx, doc_id, code, msg});
        failed++;
    }

    void add_success() { succeeded++; }
};

// ============================================================================
// Backward Compatibility Aliases
// ============================================================================

// These aliases ensure existing catch blocks continue to work
using FirestoreException = FirestoreError;
using FirestoreAuthException = FirestoreAuthError;
using FirestorePermissionException = FirestorePermissionError;
using FirestoreNotFoundException = FirestoreNotFoundError;

} // namespace duckdb
