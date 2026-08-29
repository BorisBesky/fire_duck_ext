#include "firestore_client.hpp"
#include "firestore_paging.hpp"
#include "firestore_schema_accumulator.hpp"
#include "firestore_index.hpp"
#include "firestore_types.hpp"
#include "firestore_path_utils.hpp"
#ifdef __EMSCRIPTEN__
// DuckDB-WASM has no OS sockets: route HTTP through DuckDB's HTTPUtil, which
// dispatches to the browser's fetch() (and the native HTTP stack under Node).
#include "duckdb/common/http_util.hpp"
#else
// Native builds talk to Firestore directly over httplib + OpenSSL.
#define CPPHTTPLIB_OPENSSL_SUPPORT
// Enables transparent response decompression. httplib then advertises
// "Accept-Encoding: gzip, deflate" on every request and inflates the body
// before we see it, so nothing downstream changes. Firestore's REST payloads
// are highly repetitive ({"stringValue":...} per field) and compress ~13x.
// WASM does not need this: that path goes through DuckDB's HTTPUtil to the
// browser's fetch(), which negotiates and decodes gzip on its own.
#define CPPHTTPLIB_ZLIB_SUPPORT
#include "httplib.h"
#endif
#include <sstream>
#include <cstdlib>
#include <chrono>
#include <algorithm>

namespace duckdb {

// Firestore REST API base URL template (project_id, database_id)
static const char *FIRESTORE_BASE_URL = "https://firestore.googleapis.com/v1/projects/%s/databases/%s/documents";
static const char *FIRESTORE_EMULATOR_URL = "http://%s/v1/projects/%s/databases/%s/documents";

// Check if running against the Firebase Emulator
static std::string GetEmulatorHost() {
	const char *emulator_host = std::getenv("FIRESTORE_EMULATOR_HOST");
	return emulator_host ? std::string(emulator_host) : "";
}

#ifndef __EMSCRIPTEN__
// Parse a URL into scheme+host and path components (native httplib transport only).
static bool ParseUrl(const std::string &url, std::string &scheme_host, std::string &path) {
	// Find scheme
	auto scheme_end = url.find("://");
	if (scheme_end == std::string::npos) {
		return false;
	}
	auto host_start = scheme_end + 3;
	// Find path start (first '/' after host)
	auto path_start = url.find('/', host_start);
	if (path_start == std::string::npos) {
		scheme_host = url;
		path = "/";
	} else {
		scheme_host = url.substr(0, path_start);
		path = url.substr(path_start);
	}
	return true;
}
#endif

ResolvedDocumentPath ResolveDocumentPath(const std::string &collection, const std::string &document_id) {
	ResolvedDocumentPath result;
	if (!collection.empty() && collection[0] == '~') {
		// Collection group: doc_id is already a full document path
		result.is_collection_group = true;
		result.document_path = document_id;
	} else {
		result.is_collection_group = false;
		std::string clean_collection = collection;
		if (!clean_collection.empty() && clean_collection[0] == '/') {
			clean_collection = clean_collection.substr(1);
		}
		result.document_path = clean_collection + "/" + document_id;
	}
	return result;
}

FirestoreClient::FirestoreClient(std::shared_ptr<FirestoreCredentials> credentials, DatabaseInstance &db)
    : credentials_(std::move(credentials)), db_(db) {
	if (!credentials_) {
		throw FirestoreError(FirestoreErrorCode::AUTH_CREDENTIALS_NULL, "Credentials cannot be null");
	}
	FS_LOG_DEBUG("FirestoreClient initialized for project: " + credentials_->project_id);
}

// Out of line so httplib::Client is complete at the point of destruction.
FirestoreClient::~FirestoreClient() = default;

#ifndef __EMSCRIPTEN__
httplib::Client &FirestoreClient::GetHttpClient(const std::string &scheme_host) {
	if (!http_client_ || http_client_host_ != scheme_host) {
		http_client_ = std::make_unique<httplib::Client>(scheme_host);
		http_client_host_ = scheme_host;
		// httplib defaults keep_alive_ to false, which makes it send
		// "Connection: close" and drop the socket after every response. Without
		// this the client object would be reused but the connection would not.
		http_client_->set_keep_alive(true);
		http_client_->set_connection_timeout(30);
		http_client_->set_read_timeout(30);
		FS_LOG_DEBUG("Created keep-alive HTTP client for: " + scheme_host);
	}
	return *http_client_;
}
#endif

std::string FirestoreClient::BuildBaseUrl() const {
	char buffer[512];
	std::string emulator_host = GetEmulatorHost();

	if (!emulator_host.empty()) {
		// Use emulator URL (http instead of https, custom host)
		snprintf(buffer, sizeof(buffer), FIRESTORE_EMULATOR_URL, emulator_host.c_str(),
		         credentials_->project_id.c_str(), credentials_->database_id.c_str());
		FS_LOG_DEBUG("Using emulator at: " + emulator_host);
	} else {
		// Use production Firestore URL
		snprintf(buffer, sizeof(buffer), FIRESTORE_BASE_URL, credentials_->project_id.c_str(),
		         credentials_->database_id.c_str());
	}
	return std::string(buffer);
}

std::string FirestoreClient::BuildUrl(const std::string &path) const {
	std::string url = BuildBaseUrl();
	if (!path.empty()) {
		if (path[0] != '/') {
			url += "/";
		}
		url += path;
	}
	url += credentials_->GetUrlSuffix();
	return url;
}

json FirestoreClient::MakeRequest(const std::string &method, const std::string &url, const json &body,
                                  const FirestoreErrorContext &ctx, int64_t *response_bytes_out) {
	auto start_time = std::chrono::high_resolution_clock::now();

	FS_LOG_DEBUG("Making " + method + " request to: " + url);

	// Build context for error reporting
	FirestoreErrorContext error_ctx = ctx;
	error_ctx.withMethod(method).withUrl(url).withProject(credentials_->project_id);

	// Ensure the cached token is valid (service-account OAuth2 or Firebase user ID token)
	FirestoreAuthManager::RefreshTokenIfNeeded(*credentials_, db_);

	// Serialize request body for methods that carry one
	std::string body_str;
	if (!body.empty() && (method == "POST" || method == "PATCH")) {
		body_str = body.dump();
	}

	std::string auth_header = credentials_->GetAuthHeader();

	int http_code = 0;
	std::string response_data;

#ifdef __EMSCRIPTEN__
	// WASM transport: route through DuckDB's HTTPUtil (browser fetch / Node).
	auto &http = HTTPUtil::Get(db_);
	auto params = http.InitializeParameters(db_, url);

	HTTPHeaders headers(db_);
	headers.Insert("Content-Type", "application/json");
	if (!auth_header.empty()) {
		headers.Insert("Authorization", auth_header);
	}

	unique_ptr<HTTPResponse> res;
	if (method == "GET") {
		// Accumulate the response body via the content handler.
		std::string body_accum;
		GetRequestInfo req(
		    url, headers, *params, [](const HTTPResponse &) { return true; },
		    [&](const_data_ptr_t data, idx_t len) {
			    body_accum.append(const_char_ptr_cast(data), len);
			    return true;
		    });
		req.try_request = true;
		res = http.Request(req);
		response_data = body_accum.empty() ? res->body : body_accum;
	} else if (method == "DELETE") {
		DeleteRequestInfo req(url, headers, *params);
		req.try_request = true;
		res = http.Request(req);
		response_data = res->body;
	} else {
		// POST and PATCH share the POST transport. HTTPUtil has no PATCH verb, so
		// updates go out as POST with X-HTTP-Method-Override (accepted by Firestore).
		if (method == "PATCH") {
			headers.Insert("X-HTTP-Method-Override", "PATCH");
		}
		PostRequestInfo req(url, headers, *params, const_data_ptr_cast(body_str.c_str()), body_str.size());
		req.try_request = true;
		res = http.Request(req);
		response_data = !req.buffer_out.empty() ? req.buffer_out : res->body;
	}

	if (res->HasRequestError()) {
		std::string error_msg = "HTTP request failed: " + res->GetRequestError();
		FS_LOG_ERROR(error_msg + " " + error_ctx.ToString());
		throw FirestoreNetworkError(FirestoreErrorCode::NETWORK_CURL_PERFORM, error_msg, error_ctx);
	}
	http_code = static_cast<int>(res->status);
#else
	(void)db_; // db_ is only consulted by the WASM transport above.

	// Native transport: direct httplib client over real OS sockets.
	std::string scheme_host, path;
	if (!ParseUrl(url, scheme_host, path)) {
		throw FirestoreNetworkError(FirestoreErrorCode::NETWORK_CURL_INIT, "Failed to parse URL: " + url, error_ctx);
	}

	// Reused across requests -- see GetHttpClient. Previously a Client was
	// constructed here per request, costing a TCP+TLS handshake per page.
	auto &cli = GetHttpClient(scheme_host);

	httplib::Headers headers = {{"Content-Type", "application/json"}};
	if (!auth_header.empty()) {
		headers.emplace("Authorization", auth_header);
	}

	httplib::Result res;
	if (method == "GET") {
		res = cli.Get(path, headers);
	} else if (method == "POST") {
		res = cli.Post(path, headers, body_str, "application/json");
	} else if (method == "PATCH") {
		res = cli.Patch(path, headers, body_str, "application/json");
	} else if (method == "DELETE") {
		res = cli.Delete(path, headers);
	} else {
		res = cli.Get(path, headers);
	}

	if (!res) {
		auto err = res.error();
		std::string error_msg = "HTTP request failed: " + httplib::to_string(err);
		FS_LOG_ERROR(error_msg + " " + error_ctx.ToString());
		throw FirestoreNetworkError(FirestoreErrorCode::NETWORK_CURL_PERFORM, error_msg, error_ctx);
	}

	http_code = res->status;
	response_data = res->body;
#endif

	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

	FS_LOG_DEBUG("Request completed in " + std::to_string(duration_ms) + "ms, status: " + std::to_string(http_code));

	error_ctx.withStatus(http_code);

	if (response_bytes_out != nullptr) {
		// Uncompressed: gzip is inflated by the transport before this point,
		// and it is the inflated bytes the JSON DOM is built from.
		*response_bytes_out = static_cast<int64_t>(response_data.size());
	}

	// Parse response
	json response;
	if (!response_data.empty()) {
		try {
			response = json::parse(response_data);
		} catch (const json::exception &e) {
			error_ctx.withResponseBody(response_data.substr(0, 500));
			std::string error_msg = "Failed to parse response: " + std::string(e.what());
			FS_LOG_ERROR(error_msg);
			throw FirestoreError(FirestoreErrorCode::REQUEST_RESPONSE_PARSE, error_msg, error_ctx);
		}
	}

	// Handle errors
	HandleError(http_code, response, error_ctx);

	return response;
}

void FirestoreClient::HandleError(int status_code, const json &response, const FirestoreErrorContext &ctx) {
	if (status_code >= 200 && status_code < 300) {
		return; // Success
	}

	std::string message = "Unknown error";
	if (response.contains("error")) {
		auto &error = response["error"];
		if (error.contains("message")) {
			message = error["message"].get<std::string>();
		}
	}

	FirestoreErrorContext error_ctx = ctx;
	if (response.contains("error")) {
		error_ctx.withResponseBody(response["error"].dump().substr(0, 500));
	}

	FS_LOG_ERROR("Firestore API error (HTTP " + std::to_string(status_code) + "): " + message);

	switch (status_code) {
	case 401:
		throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXPIRED, "Authentication failed: " + message,
		                         error_ctx);
	case 403:
		throw FirestorePermissionError(FirestoreErrorCode::PERMISSION_DENIED, "Permission denied: " + message,
		                               error_ctx);
	case 404:
		throw FirestoreNotFoundError(FirestoreErrorCode::NOT_FOUND_DOCUMENT, "Not found: " + message, error_ctx);
	case 429:
		throw FirestoreError(FirestoreErrorCode::REQUEST_RATE_LIMITED, "Rate limited: " + message, error_ctx);
	default:
		if (status_code >= 500) {
			throw FirestoreError(FirestoreErrorCode::REQUEST_SERVER_ERROR,
			                     "Server error (HTTP " + std::to_string(status_code) + "): " + message, error_ctx);
		}
		throw FirestoreError(FirestoreErrorCode::INTERNAL_UNEXPECTED,
		                     "HTTP " + std::to_string(status_code) + ": " + message, error_ctx);
	}
}

std::string FirestoreClient::ExtractDocumentId(const std::string &path) {
	size_t last_slash = path.rfind('/');
	if (last_slash == std::string::npos) {
		return path;
	}
	return path.substr(last_slash + 1);
}

FirestoreDocument FirestoreClient::ParseDocument(json &&doc_json) {
	FirestoreDocument doc;

	if (doc_json.contains("name")) {
		doc.name = doc_json["name"].get<std::string>();
		doc.document_id = ExtractDocumentId(doc.name);
	}

	if (doc_json.contains("fields")) {
		// Moved, not copied: a page holds up to 1000 of these, and copying
		// them while the parsed response is still alive roughly doubles peak
		// memory for the page.
		//
		// This is a trade, not a free win. Measured against the branch point
		// over 20,000 documents carrying 64-element arrays, `count(*)` costs
		// 1.408s copying and 1.652s moving -- about 17% -- with byte-identical
		// wire traffic; scalar-field collections show no difference. The
		// likely mechanism is locality: a copy produces a fresh compact
		// subtree and frees the parsed response as a block, while a move
		// leaves these fields pointing into nodes scattered through the
		// response's allocations. See bench/FINDINGS.md section 9.
		doc.fields = std::move(doc_json["fields"]);
	}

	if (doc_json.contains("createTime")) {
		doc.create_time = doc_json["createTime"].get<std::string>();
	}

	if (doc_json.contains("updateTime")) {
		doc.update_time = doc_json["updateTime"].get<std::string>();
	}

	return doc;
}

FirestoreListResponse FirestoreClient::ListDocuments(const std::string &collection, const FirestoreQuery &query) {
	FS_LOG_DEBUG("Listing documents from collection: " + collection);

	std::string url = BuildUrl(collection);

	// Add query parameters
	bool has_params = (credentials_->type == FirestoreAuthType::API_KEY);
	auto add_param = [&](const std::string &key, const std::string &value) {
		url += (has_params ? "&" : "?") + key + "=" + value;
		has_params = true;
	};

	add_param("pageSize", std::to_string(ClampFirestorePageSize(query.page_size)));

	// Note: The Firestore Emulator does not support showMissing (returns 0 results).
	// Only send showMissing=true when talking to production Firestore.
	// Firestore API does not allow showMissing and orderBy together (HTTP 400),
	// so skip showMissing when an order_by is specified.
	if (query.show_missing) {
		if (!GetEmulatorHost().empty()) {
			FS_LOG_DEBUG("show_missing=true requested but showMissing is skipped because the Firestore Emulator does "
			             "not support it.");
		} else if (query.order_by.has_value()) {
			FS_LOG_DEBUG("show_missing=true requested but showMissing is skipped because Firestore does not allow "
			             "showMissing together with orderBy; ordered scans will not include phantom documents.");
		} else {
			add_param("showMissing", "true");
		}
	}

	if (query.page_token.has_value()) {
		add_param("pageToken", query.page_token.value());
	}

	if (query.order_by.has_value()) {
		add_param("orderBy", query.order_by.value());
	}

	// Ask only for the fields the query projects, so the rest never crosses
	// the wire. A keys-only projection cannot be expressed here -- an absent
	// mask parameter means "every field", and there is no way to spell an
	// empty one in a URL -- so that case sends no mask and pays for the
	// fields it will ignore.
	if (query.projection.masked && !query.projection.KeysOnly()) {
		for (const auto &field_path : query.projection.field_paths) {
			add_param("mask.fieldPaths", UrlEncodeQueryValue(QuoteFirestoreFieldPath(field_path)));
		}
	}

	FirestoreErrorContext ctx;
	ctx.withOperation("list").withCollection(collection);

	FirestoreListResponse result;
	json response = MakeRequest("GET", url, {}, ctx, &result.response_bytes);

	if (response.contains("documents")) {
		for (auto &doc_json : response["documents"]) {
			result.documents.push_back(ParseDocument(std::move(doc_json)));
		}
	}

	if (response.contains("nextPageToken")) {
		result.next_page_token = response["nextPageToken"].get<std::string>();
	}

	FS_LOG_DEBUG("Listed " + std::to_string(result.documents.size()) + " documents");
	return result;
}

FirestoreCollectionIdsPage FirestoreClient::ListCollectionIdsPage(const std::string &document_path,
                                                                  const std::optional<std::string> &page_token,
                                                                  int64_t page_size) {
	FS_LOG_DEBUG("Listing collection IDs under document: " + document_path);

	std::string url = BuildUrl(document_path + ":listCollectionIds");

	FirestoreErrorContext ctx;
	ctx.withOperation("list_collection_ids").withCollection(document_path);

	json body = {{"pageSize", page_size}};
	if (page_token.has_value()) {
		body["pageToken"] = page_token.value();
	}

	json response = MakeRequest("POST", url, body, ctx);

	FirestoreCollectionIdsPage result;
	if (response.contains("collectionIds")) {
		for (auto &id : response["collectionIds"]) {
			result.collection_ids.push_back(id.get<std::string>());
		}
	}

	if (response.contains("nextPageToken")) {
		result.next_page_token = response["nextPageToken"].get<std::string>();
	}

	FS_LOG_DEBUG("Listed " + std::to_string(result.collection_ids.size()) + " subcollections in page");
	return result;
}

std::vector<std::string> FirestoreClient::ListCollectionIds(const std::string &document_path) {
	std::vector<std::string> collection_ids;
	std::optional<std::string> page_token;

	do {
		auto page = ListCollectionIdsPage(document_path, page_token, 100);
		collection_ids.insert(collection_ids.end(), page.collection_ids.begin(), page.collection_ids.end());

		if (page.next_page_token.empty()) {
			page_token.reset();
		} else {
			page_token = page.next_page_token;
		}
	} while (page_token.has_value());

	FS_LOG_DEBUG("Found " + std::to_string(collection_ids.size()) + " subcollections");
	return collection_ids;
}

FirestoreDocument FirestoreClient::GetDocument(const std::string &collection, const std::string &document_id) {
	FS_LOG_DEBUG("Getting document: " + collection + "/" + document_id);

	auto resolved = ResolveDocumentPath(collection, document_id);
	std::string url = BuildUrl(resolved.document_path);

	FirestoreErrorContext ctx;
	ctx.withOperation("get").withCollection(collection).withDocument(document_id);

	json response = MakeRequest("GET", url, {}, ctx);
	return ParseDocument(std::move(response));
}

FirestoreDocument FirestoreClient::CreateDocument(const std::string &collection, const json &fields,
                                                  const std::optional<std::string> &document_id) {
	FS_LOG_DEBUG("Creating document in collection: " + collection);

	std::string url = BuildUrl(collection);

	// Add document ID if specified
	if (document_id.has_value()) {
		bool has_params = (credentials_->type == FirestoreAuthType::API_KEY);
		url += (has_params ? "&" : "?") + std::string("documentId=") + document_id.value();
	}

	FirestoreErrorContext ctx;
	ctx.withOperation("create").withCollection(collection);
	if (document_id.has_value()) {
		ctx.withDocument(document_id.value());
	}

	json body = {{"fields", fields}};
	json response = MakeRequest("POST", url, body, ctx);
	return ParseDocument(std::move(response));
}

void FirestoreClient::UpdateDocument(const std::string &collection, const std::string &document_id,
                                     const json &fields) {
	FS_LOG_DEBUG("Updating document: " + collection + "/" + document_id);

	auto resolved = ResolveDocumentPath(collection, document_id);
	std::string url = BuildUrl(resolved.document_path);

	// Add updateMask for all fields
	bool has_params = (credentials_->type == FirestoreAuthType::API_KEY);
	for (auto it = fields.begin(); it != fields.end(); ++it) {
		url += (has_params ? "&" : "?") + std::string("updateMask.fieldPaths=") + it.key();
		has_params = true;
	}

	FirestoreErrorContext ctx;
	ctx.withOperation("update").withCollection(collection).withDocument(document_id);

	json body = {{"fields", fields}};
	MakeRequest("PATCH", url, body, ctx);

	FS_LOG_DEBUG("Document updated successfully");
}

void FirestoreClient::DeleteDocument(const std::string &collection, const std::string &document_id) {
	FS_LOG_DEBUG("Deleting document: " + collection + "/" + document_id);

	auto resolved = ResolveDocumentPath(collection, document_id);
	std::string url = BuildUrl(resolved.document_path);

	FirestoreErrorContext ctx;
	ctx.withOperation("delete").withCollection(collection).withDocument(document_id);

	MakeRequest("DELETE", url, {}, ctx);

	FS_LOG_DEBUG("Document deleted successfully");
}

void FirestoreClient::BatchWrite(const std::vector<json> &writes) {
	if (writes.empty()) {
		FS_LOG_DEBUG("BatchWrite called with empty writes, skipping");
		return;
	}

	FS_LOG_DEBUG("Executing batch write with " + std::to_string(writes.size()) + " operations");

	// Batch writes are sent to :batchWrite endpoint
	std::string url = BuildBaseUrl() + ":batchWrite" + credentials_->GetUrlSuffix();

	FirestoreErrorContext ctx;
	ctx.withOperation("batch_write");

	json body = {{"writes", writes}};
	MakeRequest("POST", url, body, ctx);

	FS_LOG_DEBUG("Batch write completed successfully");
}

void FirestoreClient::ArrayTransform(const std::string &collection, const std::string &document_id,
                                     const std::string &field_name, const json &elements,
                                     ArrayTransformType transform_type) {
	std::string transform_name;
	switch (transform_type) {
	case ArrayTransformType::ARRAY_UNION:
		transform_name = "appendMissingElements";
		FS_LOG_DEBUG("Array union on " + collection + "/" + document_id + "." + field_name);
		break;
	case ArrayTransformType::ARRAY_REMOVE:
		transform_name = "removeAllFromArray";
		FS_LOG_DEBUG("Array remove on " + collection + "/" + document_id + "." + field_name);
		break;
	case ArrayTransformType::ARRAY_APPEND:
		// For append, we use appendMissingElements but the caller should handle dedup
		// Actually, Firestore doesn't have a direct "append with duplicates" operation
		// We'll need to do read-modify-write for true append
		transform_name = "appendMissingElements";
		FS_LOG_DEBUG("Array append on " + collection + "/" + document_id + "." + field_name);
		break;
	}

	// Build the document path
	auto resolved = ResolveDocumentPath(collection, document_id);
	std::string doc_path = "projects/" + credentials_->project_id + "/databases/" + credentials_->database_id +
	                       "/documents/" + resolved.document_path;

	// For ARRAY_APPEND with duplicates, we need to do read-modify-write
	if (transform_type == ArrayTransformType::ARRAY_APPEND) {
		// Read current document
		FirestoreDocument current_doc = GetDocument(collection, document_id);

		// Get current array value
		json current_array = json::array();
		if (current_doc.fields.contains(field_name) && current_doc.fields[field_name].contains("arrayValue") &&
		    current_doc.fields[field_name]["arrayValue"].contains("values")) {
			current_array = current_doc.fields[field_name]["arrayValue"]["values"];
		}

		// Append new elements (allowing duplicates)
		for (const auto &elem : elements) {
			current_array.push_back(elem);
		}

		// Update with the new array
		json fields = {{field_name, {{"arrayValue", {{"values", current_array}}}}}};
		UpdateDocument(collection, document_id, fields);
		return;
	}

	// For ARRAY_UNION and ARRAY_REMOVE, use field transforms via commit
	std::string url = BuildBaseUrl() + ":commit" + credentials_->GetUrlSuffix();

	FirestoreErrorContext ctx;
	ctx.withOperation("array_transform").withCollection(collection).withDocument(document_id);

	// Build the write with field transform
	json write_op = {{"transform",
	                  {{"document", doc_path},
	                   {"fieldTransforms", {{{"fieldPath", field_name}, {transform_name, {{"values", elements}}}}}}}}};

	json body = {{"writes", {write_op}}};
	MakeRequest("POST", url, body, ctx);

	FS_LOG_DEBUG("Array transform completed successfully");
}

FirestoreListResponse FirestoreClient::CollectionGroupQuery(const std::string &collection_id,
                                                            const FirestoreQuery &query) {
	FS_LOG_DEBUG("Executing collection group query for: " + collection_id);

	// Collection group queries use the runQuery endpoint, which has no page
	// tokens -- pagination is by cursor, which is why the query is always
	// ordered (BuildCollectionGroupStructuredQuery appends __name__).
	std::string url = BuildBaseUrl() + ":runQuery" + credentials_->GetUrlSuffix();

	std::vector<OrderByField> order_by;
	if (query.order_by.has_value()) {
		order_by = ParseOrderByString(query.order_by.value());
	}

	const int64_t page_size = ClampFirestorePageSize(query.page_size);
	json structured_query = BuildCollectionGroupStructuredQuery(collection_id, order_by, page_size);
	if (query.projection.masked) {
		structured_query["select"] = BuildSelectClause(query.projection);
	}
	if (!query.start_at.is_null()) {
		structured_query["startAt"] = query.start_at;
	}

	FirestoreErrorContext ctx;
	ctx.withOperation("collection_group_query").withCollection(collection_id);

	json body = {{"structuredQuery", structured_query}};

	FirestoreListResponse result;
	json response = MakeRequest("POST", url, body, ctx, &result.response_bytes);

	// Response is an array of results, each containing a "document" field
	if (response.is_array()) {
		for (auto &item : response) {
			if (item.contains("document")) {
				result.documents.push_back(ParseDocument(std::move(item["document"])));
			}
		}
	}

	// A short page means the collection group is exhausted. A page that came
	// back full may or may not be the last one, so hand back a cursor and let
	// the next request settle it: one wasted round trip at worst, against
	// silently dropping every document past the first page.
	if (!result.documents.empty() && static_cast<int64_t>(result.documents.size()) >= page_size) {
		const auto &last_document = result.documents.back();
		result.next_start_at = BuildStartAtCursor(structured_query, last_document.name, last_document.fields);
	}

	FS_LOG_DEBUG("Collection group query returned " + std::to_string(result.documents.size()) + " documents");
	return result;
}

std::string FirestoreClient::BuildAdminUrl(const std::string &path) const {
	std::string emulator_host = GetEmulatorHost();
	std::string base;

	if (!emulator_host.empty()) {
		base = "http://" + emulator_host + "/v1/projects/" + credentials_->project_id + "/databases/" +
		       credentials_->database_id;
	} else {
		base = "https://firestore.googleapis.com/v1/projects/" + credentials_->project_id + "/databases/" +
		       credentials_->database_id;
	}

	if (!path.empty()) {
		if (path[0] != '/') {
			base += "/";
		}
		base += path;
	}

	base += credentials_->GetUrlSuffix();
	return base;
}

std::string FirestoreClient::CollectionResourceName(const std::string &collection) const {
	return "projects/" + credentials_->project_id + "/databases/" + credentials_->database_id + "/documents/" +
	       collection;
}

bool FirestoreClient::CountDocuments(const std::string &collection, bool is_collection_group, int64_t up_to,
                                     int64_t &count_out) {
	// Same URL shape as runQuery: a nested subcollection is counted against
	// its parent document path, with the final segment in collectionId.
	std::string parent_path;
	std::string collection_id;
	SplitFirestoreCollectionPath(collection, is_collection_group, parent_path, collection_id);

	std::string url = BuildBaseUrl();
	if (!parent_path.empty()) {
		url += "/" + parent_path;
	}
	url += ":runAggregationQuery" + credentials_->GetUrlSuffix();

	FirestoreErrorContext ctx;
	ctx.withOperation("count").withCollection(collection);

	json body = BuildCountAggregationQuery(collection_id, is_collection_group, up_to);
	FS_LOG_DEBUG("Counting documents in '" + collection + "' with :runAggregationQuery");

	json response = MakeRequest("POST", url, body, ctx);
	if (!ParseCountAggregationResponse(response, count_out)) {
		FS_LOG_DEBUG("Aggregation response carried no count; falling back to scanning");
		return false;
	}

	FS_LOG_DEBUG("Counted " + std::to_string(count_out) + " documents in '" + collection + "'");
	return true;
}

FirestoreListResponse FirestoreClient::RunQuery(const std::string &collection, const json &structured_query,
                                                bool is_collection_group) {
	// Nested subcollections must run the query against their parent document path
	// (".../documents/<parent>:runQuery"); the final segment is in from.collectionId.
	// Top-level collections and collection groups query from the database root.
	std::string parent_path;
	std::string collection_id;
	SplitFirestoreCollectionPath(collection, is_collection_group, parent_path, collection_id);

	FS_LOG_DEBUG("Executing runQuery for collection: " + collection +
	             " (collection_group=" + (is_collection_group ? "true" : "false") + ", parent='" + parent_path +
	             "', collectionId='" + collection_id + "')");

	std::string url = BuildBaseUrl();
	if (!parent_path.empty()) {
		url += "/" + parent_path;
	}
	url += ":runQuery" + credentials_->GetUrlSuffix();

	FirestoreErrorContext ctx;
	ctx.withOperation("run_query").withCollection(collection);

	json body = {{"structuredQuery", structured_query}};

	FS_LOG_DEBUG("StructuredQuery: " + structured_query.dump());

	FirestoreListResponse result;
	json response = MakeRequest("POST", url, body, ctx, &result.response_bytes);

	if (response.is_array()) {
		for (auto &item : response) {
			if (item.contains("document")) {
				result.documents.push_back(ParseDocument(std::move(item["document"])));
			}
		}
	}

	FS_LOG_DEBUG("RunQuery returned " + std::to_string(result.documents.size()) + " documents");
	return result;
}

std::vector<FirestoreIndex> FirestoreClient::FetchCompositeIndexes(const std::string &collection_id) {
	FS_LOG_DEBUG("Fetching composite indexes for collection: " + collection_id);

	std::string url = BuildAdminUrl("collectionGroups/" + collection_id + "/indexes");

	FirestoreErrorContext ctx;
	ctx.withOperation("fetch_indexes").withCollection(collection_id);

	json response = MakeRequest("GET", url, {}, ctx);

	std::vector<FirestoreIndex> indexes;

	if (!response.contains("indexes")) {
		FS_LOG_DEBUG("No composite indexes found for collection: " + collection_id);
		return indexes;
	}

	for (auto &idx_json : response["indexes"]) {
		FirestoreIndex idx;

		if (idx_json.contains("name")) {
			idx.name = idx_json["name"].get<std::string>();
		}

		// Parse query scope
		std::string scope_str = idx_json.value("queryScope", "COLLECTION");
		if (scope_str == "COLLECTION_GROUP") {
			idx.query_scope = FirestoreIndex::QueryScope::COLLECTION_GROUP;
		} else {
			idx.query_scope = FirestoreIndex::QueryScope::COLLECTION;
		}

		// Parse state
		std::string state_str = idx_json.value("state", "READY");
		if (state_str == "CREATING") {
			idx.state = FirestoreIndex::State::CREATING;
		} else if (state_str == "NEEDS_REPAIR") {
			idx.state = FirestoreIndex::State::NEEDS_REPAIR;
		} else {
			idx.state = FirestoreIndex::State::READY;
		}

		// Only include READY indexes
		if (idx.state != FirestoreIndex::State::READY) {
			continue;
		}

		// Parse fields
		if (idx_json.contains("fields")) {
			for (auto &field_json : idx_json["fields"]) {
				FirestoreIndexField field;
				field.field_path = field_json.value("fieldPath", "");

				if (field_json.contains("order")) {
					std::string order_str = field_json["order"].get<std::string>();
					field.mode = (order_str == "DESCENDING") ? FirestoreIndexField::Mode::DESCENDING
					                                         : FirestoreIndexField::Mode::ASCENDING;
				} else if (field_json.contains("arrayConfig")) {
					field.mode = FirestoreIndexField::Mode::ARRAY_CONTAINS;
				} else {
					field.mode = FirestoreIndexField::Mode::ASCENDING;
				}

				idx.fields.push_back(std::move(field));
			}
		}

		idx.is_single_field = (idx.fields.size() == 1);
		indexes.push_back(std::move(idx));
	}

	FS_LOG_DEBUG("Fetched " + std::to_string(indexes.size()) + " READY composite indexes");
	return indexes;
}

bool FirestoreClient::CheckDefaultSingleFieldIndexes() {
	FS_LOG_DEBUG("Checking default single-field index configuration");

	try {
		std::string url = BuildAdminUrl("collectionGroups/__default__/fields/*");

		FirestoreErrorContext ctx;
		ctx.withOperation("check_default_indexes");

		json response = MakeRequest("GET", url, {}, ctx);

		// If the response contains indexConfig with indexes, defaults are enabled
		if (response.contains("indexConfig") && response["indexConfig"].contains("indexes")) {
			auto &indexes = response["indexConfig"]["indexes"];
			if (indexes.is_array() && !indexes.empty()) {
				FS_LOG_DEBUG("Default single-field indexing is enabled (" + std::to_string(indexes.size()) +
				             " default index configs)");
				return true;
			}
		}

		FS_LOG_DEBUG("Default single-field indexing appears disabled or empty");
		return false;
	} catch (const std::exception &e) {
		// If we can't check defaults, assume they're enabled (Firestore default behavior)
		FS_LOG_DEBUG("Failed to check default index config, assuming enabled: " + std::string(e.what()));
		return true;
	}
}

std::vector<std::pair<std::string, LogicalType>> FirestoreClient::InferSchema(const std::string &collection,
                                                                              int64_t sample_size, bool show_missing,
                                                                              FirestoreMapEncoding map_encoding,
                                                                              int64_t page_size) {
	FS_LOG_DEBUG("Inferring schema for collection: " + collection);

	const bool is_collection_group = !collection.empty() && collection[0] == '~';

	// Each page is folded into the accumulator and then released, so peak
	// memory is one page however deep the sample goes. Retaining every sampled
	// document is what made `schema_sample_size := -1` unusable on a large
	// collection: it pulled the whole thing into memory at bind time, before
	// any LIMIT or filter could reduce it.
	FirestoreSchemaAccumulator accumulator(sample_size);

	std::string page_token; // documents.list pagination
	json start_at;          // runQuery (collection group) cursor pagination
	bool has_more_pages = true;

	while (has_more_pages && !accumulator.IsFull()) {
		FirestoreQuery query;
		query.show_missing = show_missing;
		const int64_t per_request = ClampFirestorePageSize(page_size);
		const int64_t remaining = accumulator.RemainingSample();
		query.page_size = remaining < 0 ? per_request : std::min(remaining, per_request);

		FirestoreListResponse page;
		if (is_collection_group) {
			// Collection groups paginate by cursor; before this they issued a
			// single request, so any sample above one page silently stopped
			// there.
			query.start_at = start_at;
			page = CollectionGroupQuery(collection.substr(1), query);
		} else {
			if (!page_token.empty()) {
				query.page_token = page_token;
			}
			page = ListDocuments(collection, query);
		}

		if (page.documents.empty()) {
			break;
		}
		for (const auto &document : page.documents) {
			accumulator.AddDocument(document.fields);
		}

		page_token = page.next_page_token;
		start_at = page.next_start_at;
		has_more_pages = page.HasMorePages();
	}

	// Convert the summary to DuckDB types.
	std::vector<std::pair<std::string, LogicalType>> result;
	for (const auto &entry : accumulator.Fields()) {
		const std::string &name = entry.first;
		const auto &summary = entry.second;

		if (summary.type_name == "arrayValue") {
			// Element type by majority of the elements actually sampled.
			LogicalType element_type = LogicalType::VARCHAR; // Default
			const std::string best_element_type =
			    FirestoreSchemaAccumulator::MajorityElementType(summary.array_element_types);
			if (best_element_type == "integerValue")
				element_type = LogicalType::BIGINT;
			else if (best_element_type == "doubleValue")
				element_type = LogicalType::DOUBLE;
			else if (best_element_type == "booleanValue")
				element_type = LogicalType::BOOLEAN;
			else if (best_element_type == "timestampValue")
				element_type = LogicalType::TIMESTAMP;
			// else keep VARCHAR
			result.emplace_back(name, LogicalType::LIST(element_type));
			FS_LOG_DEBUG("Array field '" + name + "' inferred element type: " + element_type.ToString());
		} else if (summary.type_name == "vectorValue") {
			if (summary.vector_dimension > 0) {
				result.emplace_back(name, LogicalType::ARRAY(LogicalType::DOUBLE, summary.vector_dimension));
				FS_LOG_DEBUG("Vector field '" + name +
				             "' inferred dimension: " + std::to_string(summary.vector_dimension));
			} else {
				// No occurrence told us the dimension.
				result.emplace_back(name, LogicalType::LIST(LogicalType::DOUBLE));
				FS_LOG_DEBUG("Vector field '" + name + "' could not determine dimension, using LIST(DOUBLE)");
			}
		} else {
			result.emplace_back(name, FirestoreTypeToDuckDB(summary.type_name, map_encoding));
		}
	}

	FS_LOG_DEBUG("Inferred " + std::to_string(result.size()) + " fields from " +
	             std::to_string(accumulator.DocumentsSeen()) + " documents");
	return result;
}

} // namespace duckdb
