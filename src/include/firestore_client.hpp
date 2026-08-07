#pragma once

#include "firestore_auth.hpp"
#include "firestore_error.hpp"
#include "firestore_logger.hpp"
#include "firestore_types.hpp" // FirestoreMapEncoding
#include "duckdb.hpp"
#include <nlohmann/json.hpp>
#include <vector>
#include <optional>
#include <memory>

#ifndef __EMSCRIPTEN__
// Forward declaration only: httplib.h is ~320k lines and must not be pulled
// into every translation unit that includes this header. FirestoreClient holds
// it behind a unique_ptr, so an incomplete type is sufficient here.
namespace httplib {
class Client;
} // namespace httplib
#endif

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
	bool show_missing = true; // Include phantom documents (no fields, only subcollections)
};

// Response from listing documents
struct FirestoreListResponse {
	std::vector<FirestoreDocument> documents;
	std::string next_page_token;
};

struct FirestoreCollectionIdsPage {
	std::vector<std::string> collection_ids;
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
	// `db` is used to reach DuckDB's HTTPUtil for HTTP transport on WASM builds,
	// where raw sockets are unavailable.
	FirestoreClient(std::shared_ptr<FirestoreCredentials> credentials, DatabaseInstance &db);

	// Defined out of line: destroying the unique_ptr<httplib::Client> member
	// requires httplib::Client to be complete, which it only is in the .cpp.
	~FirestoreClient();

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
	// sample_size <= 0 samples every document, paginating until the collection
	// is exhausted. Collection-group scans (~ prefix) cannot paginate and are
	// always bounded by a single request.
	std::vector<std::pair<std::string, LogicalType>> InferSchema(
	    const std::string &collection, int64_t sample_size = 1000, bool show_missing = true,
	    FirestoreMapEncoding map_encoding = FirestoreMapEncoding::WIRE);

	// Run a StructuredQuery via :runQuery endpoint (supports WHERE filters)
	FirestoreListResponse RunQuery(const std::string &collection, const json &structured_query,
	                               bool is_collection_group = false);

	FirestoreCollectionIdsPage ListCollectionIdsPage(const std::string &document_path,
	                                                 const std::optional<std::string> &page_token = std::nullopt,
	                                                 int64_t page_size = 100);

	// List subcollection IDs under a document path
	std::vector<std::string> ListCollectionIds(const std::string &document_path);

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
	// DuckDB instance handle; only consulted by the WASM HTTP transport.
	DatabaseInstance &db_;

#ifndef __EMSCRIPTEN__
	// Persistent HTTP client, reused across every request this FirestoreClient
	// makes. Constructing a client per request forces a fresh TCP (and TLS)
	// handshake for every page of a scan; keeping one alive lets the connection
	// be reused. Scans are single-threaded (FirestoreScanGlobalState::MaxThreads
	// returns 1) and each operator owns its own FirestoreClient, so no locking
	// is needed here -- httplib::Client is itself internally synchronised.
	std::unique_ptr<httplib::Client> http_client_;
	std::string http_client_host_;

	// Returns a keep-alive client bound to `scheme_host`, rebuilding it only if
	// the host changes. In practice the documents and Admin API endpoints share
	// a host, so this is built once per FirestoreClient.
	httplib::Client &GetHttpClient(const std::string &scheme_host);
#endif

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
