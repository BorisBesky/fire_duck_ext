#pragma once

#include "firestore_auth.hpp"
#include "firestore_error.hpp"
#include "firestore_logger.hpp"
#include "duckdb.hpp"
#include <nlohmann/json.hpp>
#include <vector>
#include <optional>
#include <memory>

namespace duckdb {

using json = nlohmann::json;

// Forward declaration
struct FirestoreIndex;

// Represents a Firestore document
struct FirestoreDocument {
	std::string name;        // Full document path
	std::string document_id; // Just the document ID
	json fields;             // Document fields in Firestore format
	std::string create_time;
	std::string update_time;
};

// Query parameters for listing documents
struct FirestoreQuery {
	std::optional<std::string> order_by;
	std::optional<int64_t> limit;
	std::optional<std::string> page_token;
	int64_t page_size = 1000; // Max allowed by Firestore
};

// Response from listing documents
struct FirestoreListResponse {
	std::vector<FirestoreDocument> documents;
	std::string next_page_token;
};

// Resolved document path for collection group-aware write operations
struct ResolvedDocumentPath {
	bool is_collection_group;
	std::string document_path; // relative path after /documents/
};

// Resolves a collection + document_id into the correct document path.
// For collection groups (~ prefix): strips ~, uses document_id as full path.
// For regular collections: concatenates collection/document_id.
ResolvedDocumentPath ResolveDocumentPath(const std::string &collection, const std::string &document_id);

class FirestoreClient {
public:
	explicit FirestoreClient(std::shared_ptr<FirestoreCredentials> credentials);

	// Read operations
	FirestoreListResponse ListDocuments(const std::string &collection, const FirestoreQuery &query = {});

	FirestoreDocument GetDocument(const std::string &collection, const std::string &document_id);

	// Write operations
	FirestoreDocument CreateDocument(const std::string &collection, const json &fields,
	                                 const std::optional<std::string> &document_id = std::nullopt);

	void UpdateDocument(const std::string &collection, const std::string &document_id, const json &fields);

	void DeleteDocument(const std::string &collection, const std::string &document_id);

	// Batch write for bulk operations
	void BatchWrite(const std::vector<json> &writes);

	// Array field transforms
	enum class ArrayTransformType {
		ARRAY_UNION,  // Add elements without duplicates
		ARRAY_REMOVE, // Remove specific elements
		ARRAY_APPEND  // Append elements (may create duplicates)
	};

	void ArrayTransform(const std::string &collection, const std::string &document_id, const std::string &field_name,
	                    const json &elements, ArrayTransformType transform_type);

	// Collection group query - queries all subcollections with a given name
	// Use this to query across all documents in subcollections
	FirestoreListResponse CollectionGroupQuery(const std::string &collection_id, const FirestoreQuery &query = {});

	// Infer schema from sample documents
	// Use ~ prefix for collection group queries (e.g., "~profile")
	// Returns pairs of (field_name, DuckDB LogicalType)
	std::vector<std::pair<std::string, LogicalType>> InferSchema(const std::string &collection,
	                                                             int64_t sample_size = 100);

	// Run a StructuredQuery via :runQuery endpoint (supports WHERE filters)
	FirestoreListResponse RunQuery(const std::string &collection, const json &structured_query,
	                               bool is_collection_group = false);

	// Fetch composite indexes for a collection via Admin API
	std::vector<FirestoreIndex> FetchCompositeIndexes(const std::string &collection_id);

	// Check if default single-field indexing is enabled via Admin API
	bool CheckDefaultSingleFieldIndexes();

	// Get project ID
	const std::string &GetProjectId() const {
		return credentials_->project_id;
	}

private:
	std::shared_ptr<FirestoreCredentials> credentials_;

	// Build base URL for Firestore REST API (documents endpoint)
	std::string BuildBaseUrl() const;

	// Build full URL with path (documents endpoint)
	std::string BuildUrl(const std::string &path) const;

	// Build URL for Admin API (indexes, fields, etc.)
	std::string BuildAdminUrl(const std::string &path) const;

	// Make HTTP request with error context
	json MakeRequest(const std::string &method, const std::string &url, const json &body = {},
	                 const FirestoreErrorContext &ctx = {});

	// Handle error response with context
	void HandleError(int status_code, const json &response, const FirestoreErrorContext &ctx);

	// Parse document from JSON response
	FirestoreDocument ParseDocument(const json &doc_json);

	// Extract document ID from full path
	std::string ExtractDocumentId(const std::string &path);
};

} // namespace duckdb
