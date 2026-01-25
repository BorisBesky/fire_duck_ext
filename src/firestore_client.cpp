#include "firestore_client.hpp"
#include "firestore_types.hpp"
#include <curl/curl.h>
#include <sstream>
#include <cstdlib>
#include <chrono>

namespace duckdb {

// Firestore REST API base URL template (project_id, database_id)
static const char* FIRESTORE_BASE_URL = "https://firestore.googleapis.com/v1/projects/%s/databases/%s/documents";
static const char* FIRESTORE_EMULATOR_URL = "http://%s/v1/projects/%s/databases/%s/documents";

// Check if running against the Firebase Emulator
static std::string GetEmulatorHost() {
    const char* emulator_host = std::getenv("FIRESTORE_EMULATOR_HOST");
    return emulator_host ? std::string(emulator_host) : "";
}

// CURL write callback
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* s) {
    size_t newLength = size * nmemb;
    s->append((char*)contents, newLength);
    return newLength;
}

FirestoreClient::FirestoreClient(std::shared_ptr<FirestoreCredentials> credentials)
    : credentials_(std::move(credentials)) {
    if (!credentials_) {
        throw FirestoreError(FirestoreErrorCode::AUTH_CREDENTIALS_NULL,
                             "Credentials cannot be null");
    }
    FS_LOG_DEBUG("FirestoreClient initialized for project: " + credentials_->project_id);
}

std::string FirestoreClient::BuildBaseUrl() const {
    char buffer[512];
    std::string emulator_host = GetEmulatorHost();

    if (!emulator_host.empty()) {
        // Use emulator URL (http instead of https, custom host)
        snprintf(buffer, sizeof(buffer), FIRESTORE_EMULATOR_URL,
                 emulator_host.c_str(),
                 credentials_->project_id.c_str(),
                 credentials_->database_id.c_str());
        FS_LOG_DEBUG("Using emulator at: " + emulator_host);
    } else {
        // Use production Firestore URL
        snprintf(buffer, sizeof(buffer), FIRESTORE_BASE_URL,
                 credentials_->project_id.c_str(),
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

json FirestoreClient::MakeRequest(const std::string &method, const std::string &url,
                                   const json &body, const FirestoreErrorContext &ctx) {
    auto start_time = std::chrono::high_resolution_clock::now();

    FS_LOG_DEBUG("Making " + method + " request to: " + url);

    // Build context for error reporting
    FirestoreErrorContext error_ctx = ctx;
    error_ctx.withMethod(method).withUrl(url).withProject(credentials_->project_id);

    // Ensure token is valid for service account auth
    FirestoreAuthManager::RefreshTokenIfNeeded(*credentials_);

    CURL *curl = curl_easy_init();
    if (!curl) {
        throw FirestoreNetworkError(FirestoreErrorCode::NETWORK_CURL_INIT,
                                    "Failed to initialize CURL", error_ctx);
    }

    std::string response_data;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_data);

    // Set method
    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
    } else if (method == "PATCH") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
    } else if (method == "DELETE") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    }
    // GET is default

    // Set headers
    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    std::string auth_header = credentials_->GetAuthHeader();
    if (!auth_header.empty()) {
        std::string auth_line = "Authorization: " + auth_header;
        headers = curl_slist_append(headers, auth_line.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    // Set body for POST/PATCH
    std::string body_str;
    if (!body.empty() && (method == "POST" || method == "PATCH")) {
        body_str = body.dump();
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_str.c_str());
    }

    CURLcode res = curl_easy_perform(curl);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    FS_LOG_DEBUG("Request completed in " + std::to_string(duration_ms) + "ms, status: " + std::to_string(http_code));

    error_ctx.withStatus(static_cast<int>(http_code));

    if (res != CURLE_OK) {
        error_ctx.withResponseBody(response_data);
        std::string error_msg = "HTTP request failed: " + std::string(curl_easy_strerror(res));
        FS_LOG_ERROR(error_msg + " " + error_ctx.ToString());
        throw FirestoreNetworkError(FirestoreErrorCode::NETWORK_CURL_PERFORM, error_msg, error_ctx);
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
        return;  // Success
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
            throw FirestoreAuthError(FirestoreErrorCode::AUTH_TOKEN_EXPIRED,
                                     "Authentication failed: " + message, error_ctx);
        case 403:
            throw FirestorePermissionError(FirestoreErrorCode::PERMISSION_DENIED,
                                           "Permission denied: " + message, error_ctx);
        case 404:
            throw FirestoreNotFoundError(FirestoreErrorCode::NOT_FOUND_DOCUMENT,
                                         "Not found: " + message, error_ctx);
        case 429:
            throw FirestoreError(FirestoreErrorCode::REQUEST_RATE_LIMITED,
                                 "Rate limited: " + message, error_ctx);
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

FirestoreDocument FirestoreClient::ParseDocument(const json &doc_json) {
    FirestoreDocument doc;

    if (doc_json.contains("name")) {
        doc.name = doc_json["name"].get<std::string>();
        doc.document_id = ExtractDocumentId(doc.name);
    }

    if (doc_json.contains("fields")) {
        doc.fields = doc_json["fields"];
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

    add_param("pageSize", std::to_string(query.page_size));

    if (query.page_token.has_value()) {
        add_param("pageToken", query.page_token.value());
    }

    if (query.order_by.has_value()) {
        add_param("orderBy", query.order_by.value());
    }

    FirestoreErrorContext ctx;
    ctx.withOperation("list").withCollection(collection);

    json response = MakeRequest("GET", url, {}, ctx);

    FirestoreListResponse result;

    if (response.contains("documents")) {
        for (auto &doc_json : response["documents"]) {
            result.documents.push_back(ParseDocument(doc_json));
        }
    }

    if (response.contains("nextPageToken")) {
        result.next_page_token = response["nextPageToken"].get<std::string>();
    }

    FS_LOG_DEBUG("Listed " + std::to_string(result.documents.size()) + " documents");
    return result;
}

FirestoreDocument FirestoreClient::GetDocument(const std::string &collection, const std::string &document_id) {
    FS_LOG_DEBUG("Getting document: " + collection + "/" + document_id);

    std::string path = collection + "/" + document_id;
    std::string url = BuildUrl(path);

    FirestoreErrorContext ctx;
    ctx.withOperation("get").withCollection(collection).withDocument(document_id);

    json response = MakeRequest("GET", url, {}, ctx);
    return ParseDocument(response);
}

FirestoreDocument FirestoreClient::CreateDocument(
    const std::string &collection,
    const json &fields,
    const std::optional<std::string> &document_id
) {
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
    return ParseDocument(response);
}

void FirestoreClient::UpdateDocument(
    const std::string &collection,
    const std::string &document_id,
    const json &fields
) {
    FS_LOG_DEBUG("Updating document: " + collection + "/" + document_id);

    std::string path = collection + "/" + document_id;
    std::string url = BuildUrl(path);

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

    std::string path = collection + "/" + document_id;
    std::string url = BuildUrl(path);

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

FirestoreListResponse FirestoreClient::CollectionGroupQuery(
    const std::string &collection_id,
    const FirestoreQuery &query
) {
    FS_LOG_DEBUG("Executing collection group query for: " + collection_id);

    // Collection group queries use the runQuery endpoint
    // This queries all collections/subcollections with the given collection ID
    std::string url = BuildBaseUrl() + ":runQuery" + credentials_->GetUrlSuffix();

    // Build structured query for collection group
    json structured_query = {
        {"from", {{
            {"collectionId", collection_id},
            {"allDescendants", true}  // This makes it a collection group query
        }}},
        {"limit", query.page_size}
    };

    if (query.order_by.has_value()) {
        // Parse order_by (e.g., "createdAt DESC")
        std::string order_str = query.order_by.value();
        std::string field_name = order_str;
        std::string direction = "ASCENDING";

        size_t space_pos = order_str.find(' ');
        if (space_pos != std::string::npos) {
            field_name = order_str.substr(0, space_pos);
            std::string dir_str = order_str.substr(space_pos + 1);
            if (dir_str == "DESC" || dir_str == "desc") {
                direction = "DESCENDING";
            }
        }

        structured_query["orderBy"] = {{
            {"field", {{"fieldPath", field_name}}},
            {"direction", direction}
        }};
    }

    FirestoreErrorContext ctx;
    ctx.withOperation("collection_group_query").withCollection(collection_id);

    json body = {{"structuredQuery", structured_query}};

    json response = MakeRequest("POST", url, body, ctx);

    FirestoreListResponse result;

    // Response is an array of results, each containing a "document" field
    if (response.is_array()) {
        for (auto &item : response) {
            if (item.contains("document")) {
                result.documents.push_back(ParseDocument(item["document"]));
            }
        }
    }

    FS_LOG_DEBUG("Collection group query returned " + std::to_string(result.documents.size()) + " documents");
    return result;
}

std::vector<std::pair<std::string, LogicalType>> FirestoreClient::InferSchema(
    const std::string &collection,
    int64_t sample_size
) {
    FS_LOG_DEBUG("Inferring schema for collection: " + collection);

    FirestoreQuery query;
    query.page_size = std::min(sample_size, static_cast<int64_t>(1000));

    FirestoreListResponse response;

    // Check if this is a collection group query (starts with ~)
    if (!collection.empty() && collection[0] == '~') {
        // Collection group query - extract collection ID
        std::string collection_id = collection.substr(1);
        response = CollectionGroupQuery(collection_id, query);
    } else {
        // Normal collection query
        response = ListDocuments(collection, query);
    }

    // Collect all field names and their types
    std::map<std::string, std::string> field_types;
    // For array fields, collect element types
    std::map<std::string, std::map<std::string, int64_t>> array_element_types;

    for (const auto &doc : response.documents) {
        for (auto it = doc.fields.begin(); it != doc.fields.end(); ++it) {
            const std::string &field_name = it.key();
            const json &field_value = it.value();

            // Determine type from Firestore value format
            std::string type_name;
            if (field_value.contains("stringValue")) type_name = "stringValue";
            else if (field_value.contains("integerValue")) type_name = "integerValue";
            else if (field_value.contains("doubleValue")) type_name = "doubleValue";
            else if (field_value.contains("booleanValue")) type_name = "booleanValue";
            else if (field_value.contains("timestampValue")) type_name = "timestampValue";
            else if (field_value.contains("geoPointValue")) type_name = "geoPointValue";
            else if (field_value.contains("arrayValue")) {
                type_name = "arrayValue";
                // Sample array elements to determine element type
                if (field_value["arrayValue"].contains("values")) {
                    for (const auto &elem : field_value["arrayValue"]["values"]) {
                        std::string elem_type;
                        if (elem.contains("stringValue")) elem_type = "stringValue";
                        else if (elem.contains("integerValue")) elem_type = "integerValue";
                        else if (elem.contains("doubleValue")) elem_type = "doubleValue";
                        else if (elem.contains("booleanValue")) elem_type = "booleanValue";
                        else if (elem.contains("timestampValue")) elem_type = "timestampValue";
                        else if (elem.contains("nullValue")) continue;  // Skip nulls for type inference
                        else elem_type = "stringValue";  // Default
                        array_element_types[field_name][elem_type]++;
                    }
                }
            }
            else if (field_value.contains("mapValue")) type_name = "mapValue";
            else if (field_value.contains("referenceValue")) type_name = "referenceValue";
            else if (field_value.contains("bytesValue")) type_name = "bytesValue";
            else if (field_value.contains("nullValue")) type_name = "nullValue";
            else type_name = "stringValue";  // Default

            // Store first seen type (could be improved to handle type conflicts)
            if (field_types.find(field_name) == field_types.end()) {
                field_types[field_name] = type_name;
            }
        }
    }

    // Convert to vector with proper LogicalTypes
    std::vector<std::pair<std::string, LogicalType>> result;
    for (const auto &[name, type] : field_types) {
        if (type == "arrayValue") {
            // Determine element type from sampled elements
            LogicalType element_type = LogicalType::VARCHAR;  // Default
            if (array_element_types.count(name)) {
                std::string best_elem_type = "stringValue";
                int64_t best_count = 0;
                for (const auto &[elem_type, cnt] : array_element_types[name]) {
                    if (cnt > best_count) {
                        best_count = cnt;
                        best_elem_type = elem_type;
                    }
                }
                if (best_elem_type == "integerValue") element_type = LogicalType::BIGINT;
                else if (best_elem_type == "doubleValue") element_type = LogicalType::DOUBLE;
                else if (best_elem_type == "booleanValue") element_type = LogicalType::BOOLEAN;
                else if (best_elem_type == "timestampValue") element_type = LogicalType::TIMESTAMP;
                // else keep VARCHAR
            }
            result.emplace_back(name, LogicalType::LIST(element_type));
            FS_LOG_DEBUG("Array field '" + name + "' inferred element type: " + element_type.ToString());
        } else {
            result.emplace_back(name, FirestoreTypeToDuckDB(type));
        }
    }

    FS_LOG_DEBUG("Inferred " + std::to_string(result.size()) + " fields from " +
                 std::to_string(response.documents.size()) + " documents");
    return result;
}

} // namespace duckdb
