#include "firestore_scanner.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "firestore_settings.hpp"
#include "firestore_logger.hpp"
#include "firestore_error.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <algorithm>
#include <chrono>

namespace duckdb {

// Cached schema entry with timestamp
struct CachedSchemaEntry {
	std::vector<std::pair<std::string, LogicalType>> schema;
	std::shared_ptr<FirestoreIndexCache> index_cache;
	std::chrono::steady_clock::time_point cached_at;

	bool IsExpired(int64_t ttl_seconds) const {
		if (ttl_seconds == 0) {
			return true; // Cache disabled
		}
		auto age = std::chrono::steady_clock::now() - cached_at;
		return age > std::chrono::seconds(ttl_seconds);
	}
};

// Cache of inferred schemas and index metadata for collections
static std::unordered_map<std::string, CachedSchemaEntry> schema_cache;
static std::mutex schema_cache_mutex;
// Function to clear the schema cache (exposed for testing/manual refresh)
// If collection is empty, clears entire cache. Otherwise clears entries matching the collection.
void ClearFirestoreSchemaCache(const std::string &collection) {
	std::lock_guard<std::mutex> lock(schema_cache_mutex);
	if (collection.empty()) {
		schema_cache.clear();
		FS_LOG_DEBUG("Schema cache cleared (all entries)");
	} else {
		// Clear entries that match the collection (cache key format is "project_id:database_id:collection")
		auto it = schema_cache.begin();
		int cleared = 0;
		while (it != schema_cache.end()) {
			// Check if the cache key ends with ":collection"
			const std::string &key = it->first;
			size_t colon_pos = key.rfind(':'); // Find LAST colon to extract collection
			if (colon_pos != std::string::npos) {
				std::string cached_collection = key.substr(colon_pos + 1);
				if (cached_collection == collection) {
					it = schema_cache.erase(it);
					cleared++;
					continue;
				}
			}
			++it;
		}
		FS_LOG_DEBUG("Schema cache cleared for collection '" + collection + "': " + std::to_string(cleared) +
		             " entries removed");
		if (cleared == 0) {
			FS_LOG_WARN("No cache entries found for collection: " + collection);
		}
	}
}

// Format a pushdown filter for EXPLAIN output
static string FormatPushdownFilter(const FirestorePushdownFilter &f) {
	if (f.is_unary) {
		return f.field_path + " " + f.unary_op;
	}
	if (f.is_in_filter) {
		return f.field_path + " " + f.firestore_op + " [" + std::to_string(f.in_values.size()) + " values]";
	}
	// Format the value compactly
	string val_str;
	if (f.firestore_value.contains("stringValue")) {
		val_str = "'" + f.firestore_value["stringValue"].get<string>() + "'";
	} else if (f.firestore_value.contains("integerValue")) {
		val_str = f.firestore_value["integerValue"].get<string>();
	} else if (f.firestore_value.contains("doubleValue")) {
		val_str = std::to_string(f.firestore_value["doubleValue"].get<double>());
	} else if (f.firestore_value.contains("booleanValue")) {
		val_str = f.firestore_value["booleanValue"].get<bool>() ? "true" : "false";
	} else if (f.firestore_value.contains("nullValue")) {
		val_str = "NULL";
	} else {
		val_str = f.firestore_value.dump();
	}
	return f.field_path + " " + f.firestore_op + " " + val_str;
}

// Returns true if the order_by fields have supporting indexes for the query's scope.
// Single-field ordering uses single-field indexes; multi-field ordering requires composite indexes.
// When no index exists (e.g. collection group without explicit index), returns false
// so the caller can skip server-side ordering and let DuckDB sort client-side.
static bool CanOrderByOnServer(const std::vector<OrderByField> &order_by_fields,
                               const FirestoreScanBindData &bind_data) {
	if (order_by_fields.empty()) {
		return false;
	}
	if (!bind_data.index_cache || !bind_data.index_cache->fetch_succeeded) {
		return true; // Conservative: assume ordering works when index info unavailable
	}

	auto scope = bind_data.is_collection_group ? FirestoreIndex::QueryScope::COLLECTION_GROUP
	                                           : FirestoreIndex::QueryScope::COLLECTION;

	if (order_by_fields.size() == 1) {
		// Single-field: use single-field index check
		return HasSingleFieldIndex(order_by_fields[0].field_path, *bind_data.index_cache, scope);
	}

	// Multi-field: requires composite index with matching field order and directions
	return HasCompositeOrderByIndex(order_by_fields, *bind_data.index_cache, scope);
}

// pushdown_complex_filter callback: extracts filter expressions for Firestore pushdown
// but leaves all expressions in the vector so DuckDB re-applies them as post-scan filters.
// This ensures correctness - Firestore filtering is used for performance (reduce network transfer)
// while DuckDB re-verifies every row (handles semantic differences like nulls, empty strings, etc.)
static void FirestoreComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                           vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<FirestoreScanBindData>();

	if (!bind_data.index_cache || !bind_data.index_cache->fetch_succeeded) {
		return;
	}

	bind_data.candidate_pushdown_filters.clear();

	// Build column_id_map: maps binding.column_index -> original column index in get.names
	// binding.column_index is the position in LogicalGet's column_ids array
	std::vector<idx_t> column_id_map;
	auto &col_ids = get.GetColumnIds();
	column_id_map.reserve(col_ids.size());
	for (auto &cid : col_ids) {
		column_id_map.push_back(cid.GetPrimaryIndex());
	}

	for (auto &filter : filters) {
		auto converted =
		    ConvertExpressionToFilters(*filter, get.table_index, get.names, get.returned_types, column_id_map);
		bind_data.candidate_pushdown_filters.insert(bind_data.candidate_pushdown_filters.end(),
		                                            std::make_move_iterator(converted.begin()),
		                                            std::make_move_iterator(converted.end()));
	}

	// Match against indexes now to populate EXPLAIN output
	if (!bind_data.candidate_pushdown_filters.empty()) {
		bool is_collection_group = !bind_data.collection.empty() && bind_data.collection[0] == '~';
		auto result =
		    MatchFiltersToIndexes(bind_data.candidate_pushdown_filters, *bind_data.index_cache, is_collection_group);

		if (result.has_pushdown()) {
			string info;
			for (auto &f : result.pushed_filters) {
				if (!info.empty()) {
					info += ", ";
				}
				info += FormatPushdownFilter(f);
			}
			// Prepend filter info, preserving any ORDER BY/LIMIT pushdown info
			// already set by the optimizer extension
			auto &existing = get.extra_info.file_filters;
			if (!existing.empty()) {
				existing = "Firestore Pushed Filters: " + info + " | " + existing;
			} else {
				existing = "Firestore Pushed Filters: " + info;
			}
		}
	}

	// IMPORTANT: Do NOT remove any expressions from the filters vector.
	// By leaving them all in place, DuckDB will apply them as post-scan filters,
	// ensuring correctness regardless of Firestore's filtering semantics.
}

// Count the number of path segments (split by '/'), ignoring leading/trailing slashes.
// Odd segments = collection path (e.g. "users" or "users/uid/orders")
// Even segments = document path (e.g. "users/uid" or "users/uid/orders/oid")
static int CountPathSegments(const std::string &path) {
	int count = 0;
	bool in_segment = false;
	for (char c : path) {
		if (c == '/') {
			in_segment = false;
		} else if (!in_segment) {
			in_segment = true;
			count++;
		}
	}
	return count;
}

static bool IsDocumentPath(const std::string &path) {
	int segments = CountPathSegments(path);
	return segments >= 2 && segments % 2 == 0;
}

static std::string TrimWhitespace(const std::string &s) {
	size_t start = s.find_first_not_of(" \t\r\n");
	if (start == std::string::npos) {
		return "";
	}
	size_t end = s.find_last_not_of(" \t\r\n");
	return s.substr(start, end - start + 1);
}

static bool TryGetDocPathOrderType(const std::string &order_by, DocPathOrderType &out_order) {
	std::string trimmed = TrimWhitespace(order_by);
	if (trimmed.empty() || trimmed.find(',') != std::string::npos) {
		return false;
	}

	size_t space = trimmed.find(' ');
	std::string field_name = space == std::string::npos ? trimmed : trimmed.substr(0, space);
	if (field_name != "__document_id") {
		return false;
	}

	std::string direction = space == std::string::npos ? "" : TrimWhitespace(trimmed.substr(space + 1));
	if (direction.empty() || direction == "ASC" || direction == "asc" || direction == "ASCENDING" ||
	    direction == "ascending") {
		out_order = DocPathOrderType::ASCENDING;
		return true;
	}
	if (direction == "DESC" || direction == "desc" || direction == "DESCENDING" || direction == "descending") {
		out_order = DocPathOrderType::DESCENDING;
		return true;
	}
	return false;
}

static bool TryGetDocPathOrderType(const std::vector<OrderByField> &order_by_fields, DocPathOrderType &out_order) {
	if (order_by_fields.size() != 1) {
		return false;
	}
	if (order_by_fields[0].field_path != "__document_id") {
		return false;
	}
	out_order =
	    order_by_fields[0].direction == "DESCENDING" ? DocPathOrderType::DESCENDING : DocPathOrderType::ASCENDING;
	return true;
}

static void SortDocPathIds(std::vector<std::string> &ids, DocPathOrderType order) {
	if (order == DocPathOrderType::NONE) {
		return;
	}
	std::sort(ids.begin(), ids.end());
	if (order == DocPathOrderType::DESCENDING) {
		std::reverse(ids.begin(), ids.end());
	}
}

static std::vector<std::string> FetchDocPathIds(FirestoreClient &client, const std::string &document_path) {
	std::vector<std::string> ids;
	std::optional<std::string> page_token;
	do {
		auto page = client.ListCollectionIdsPage(document_path, page_token, 100);
		for (auto &id : page.collection_ids) {
			ids.push_back(id);
		}

		if (page.next_page_token.empty()) {
			page_token.reset();
		} else {
			page_token = page.next_page_token;
		}
	} while (page_token.has_value());

	return ids;
}

void RegisterFirestoreScanFunction(ExtensionLoader &loader) {
	TableFunction scan_func("firestore_scan", {LogicalType::VARCHAR}, // collection name (required)
	                        FirestoreScanFunction, FirestoreScanBind, FirestoreScanInitGlobal, FirestoreScanInitLocal);

	// Add named parameters
	scan_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	scan_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	scan_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	scan_func.named_parameters["database"] = LogicalType::VARCHAR;
	scan_func.named_parameters["scan_limit"] = LogicalType::BIGINT;
	scan_func.named_parameters["order_by"] = LogicalType::VARCHAR;
	scan_func.named_parameters["show_missing"] = LogicalType::BOOLEAN;

	// Enable projection pushdown for efficiency
	scan_func.projection_pushdown = true;

	// Use complex filter pushdown: this lets us extract filter info for Firestore queries
	// while leaving all expressions for DuckDB to re-verify (ensuring correct results)
	scan_func.pushdown_complex_filter = FirestoreComplexFilterPushdown;

	loader.RegisterFunction(scan_func);
}

unique_ptr<FunctionData> FirestoreScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FirestoreScanBindData>();

	// Get collection name (required first positional argument)
	result->collection = input.inputs[0].GetValue<string>();
	FS_LOG_DEBUG("[DEBUG-BIND] collection='%s'", result->collection.c_str());

	// Process named parameters
	std::optional<std::string> project_id;
	std::optional<std::string> credentials_path;
	std::optional<std::string> api_key;
	std::optional<std::string> database_id;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "project_id") {
			project_id = kv.second.GetValue<string>();
		} else if (kv.first == "credentials") {
			credentials_path = kv.second.GetValue<string>();
		} else if (kv.first == "api_key") {
			api_key = kv.second.GetValue<string>();
		} else if (kv.first == "database") {
			database_id = kv.second.GetValue<string>();
		} else if (kv.first == "scan_limit") {
			result->limit = kv.second.GetValue<int64_t>();
		} else if (kv.first == "order_by") {
			result->order_by = kv.second.GetValue<string>();
			result->parsed_order_by = ParseOrderByString(result->order_by.value());
		} else if (kv.first == "show_missing") {
			result->show_missing = kv.second.GetValue<bool>();
		}
	}

	// Resolve credentials from various sources
	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException(
		    "No Firestore credentials found. Provide credentials parameter, "
		    "create a secret with CREATE SECRET, or set GOOGLE_APPLICATION_CREDENTIALS environment variable.");
	}

	// Detect document paths (even number of segments like "users/uid" or "artifacts/default-app-id").
	// Return a single __document_id column and defer subcollection lookup until execution time.
	bool is_collection_group = !result->collection.empty() && result->collection[0] == '~';
	if (!is_collection_group && IsDocumentPath(result->collection)) {
		// Return subcollection names as virtual __document_id rows.
		result->is_document_path = true;
		DocPathOrderType docpath_order = DocPathOrderType::NONE;
		bool has_supported_docpath_order = result->order_by.has_value()
		                                       ? TryGetDocPathOrderType(result->order_by.value(), docpath_order)
		                                       : TryGetDocPathOrderType(result->parsed_order_by, docpath_order);
		if (result->order_by.has_value() || !result->parsed_order_by.empty()) {
			if (!has_supported_docpath_order) {
				throw BinderException(
				    "Document-path scans only support order_by on __document_id with an optional ASC/DESC direction");
			}
			result->docpath_named_order = docpath_order;
		}

		names.push_back("__document_id");
		return_types.push_back(LogicalType::VARCHAR);
		return std::move(result);
	}

	std::string cache_key =
	    result->credentials->project_id + ":" + result->credentials->database_id + ":" + result->collection;
	int64_t ttl_seconds = FirestoreSettings::SchemaCacheTTLSeconds(context);
	{
		std::lock_guard<std::mutex> lock(schema_cache_mutex);
		auto it = schema_cache.find(cache_key);
		if (it != schema_cache.end() && !it->second.IsExpired(ttl_seconds)) {
			// Check if cached schema is empty (collection was empty when cached)
			// If so, remove stale entry and re-infer to get fresh error
			if (it->second.schema.empty()) {
				FS_LOG_DEBUG("Removing empty cached schema for collection: " + result->collection);
				schema_cache.erase(it);
				// Fall through to re-infer schema
			} else {
				// Schema found in cache and not expired, use it
				FS_LOG_DEBUG("Schema found in cache for collection: " + result->collection);
				FS_LOG_DEBUG("Cache key: " + cache_key);
				FS_LOG_DEBUG("Schema cache hit, columns: " + std::to_string(it->second.schema.size()));

				// Always include __document_id as first column
				names.push_back("__document_id");
				return_types.push_back(LogicalType::VARCHAR);

				for (const auto &[col_name, col_type] : it->second.schema) {
					names.push_back(col_name);
					return_types.push_back(col_type);
					result->column_names.push_back(col_name);
					result->column_types.push_back(col_type);
				}

				// Also restore index cache if available
				if (it->second.index_cache) {
					result->index_cache = it->second.index_cache;
					FS_LOG_DEBUG("Index cache restored from cache");
				}

				return std::move(result);
			}
		} else if (it != schema_cache.end()) {
			// Cache entry expired, remove it
			FS_LOG_DEBUG("Schema cache expired for collection: " + result->collection);
			schema_cache.erase(it);
		}
	}

	// Create client and infer schema from collection
	FirestoreClient client(result->credentials);
	auto schema = client.InferSchema(result->collection, 100, result->show_missing);

	// Check if collection exists (has documents)
	if (schema.empty()) {
		// When show_missing is enabled, an empty schema means all documents are phantom
		// (no fields, only subcollections). Return only __document_id column so users
		// can discover document IDs.
		if (result->show_missing) {
			FS_LOG_DEBUG("Collection '" + result->collection +
			             "' contains only phantom/missing documents (no fields). "
			             "Returning __document_id only.");
		} else {
			bool is_collection_group = !result->collection.empty() && result->collection[0] == '~';
			std::string collection_type = is_collection_group ? "Collection group" : "Collection";
			std::string display_name = is_collection_group ? result->collection.substr(1) : result->collection;

			FirestoreErrorContext ctx;
			ctx.withCollection(result->collection).withProject(result->credentials->project_id).withOperation("scan");

			throw FirestoreNotFoundError(
			    FirestoreErrorCode::NOT_FOUND_COLLECTION,
			    collection_type + " '" + display_name + "' does not exist or contains no documents.", ctx);
		}
	}

	// Always include __document_id as first column
	names.push_back("__document_id");
	return_types.push_back(LogicalType::VARCHAR);

	// Add inferred columns
	for (const auto &[col_name, col_type] : schema) {
		names.push_back(col_name);
		return_types.push_back(col_type);
		result->column_names.push_back(col_name);
		result->column_types.push_back(col_type);
	}

	// Fetch index metadata for filter pushdown
	result->index_cache = std::make_shared<FirestoreIndexCache>();
	try {
		// Determine collection ID for index lookup
		std::string collection_id = result->collection;
		if (!collection_id.empty() && collection_id[0] == '~') {
			collection_id = collection_id.substr(1);
		}
		// For nested paths like "users/user1/orders", use the last segment
		size_t last_slash = collection_id.rfind('/');
		if (last_slash != std::string::npos) {
			collection_id = collection_id.substr(last_slash + 1);
		}

		result->index_cache->composite_indexes = client.FetchCompositeIndexes(collection_id);
		result->index_cache->default_single_field_enabled = client.CheckDefaultSingleFieldIndexes();
		result->index_cache->fetch_succeeded = true;
		FS_LOG_DEBUG("Index cache populated: " + std::to_string(result->index_cache->composite_indexes.size()) +
		             " composite indexes, default_single_field=" +
		             (result->index_cache->default_single_field_enabled ? "true" : "false"));
	} catch (const std::exception &e) {
		// Admin API unavailable (e.g. emulator, insufficient permissions).
		// Assume Firestore's default single-field indexes exist for every field.
		// Composite index queries may fail at RunQuery time, but the existing
		// fallback (catch in InitGlobal) handles that gracefully.
		FS_LOG_WARN("Failed to fetch indexes (Admin API unavailable): " + std::string(e.what()) +
		            ". Assuming default single-field indexes.");
		result->index_cache->fetch_succeeded = true;
		result->index_cache->default_single_field_enabled = true;
	}

	// Store schema and index cache for future queries
	{
		std::lock_guard<std::mutex> lock(schema_cache_mutex);
		CachedSchemaEntry entry;
		entry.schema = schema;
		entry.index_cache = result->index_cache;
		entry.cached_at = std::chrono::steady_clock::now();
		schema_cache[cache_key] = std::move(entry);
		FS_LOG_DEBUG("Schema cached for: " + cache_key);
	}

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> FirestoreScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<FirestoreScanBindData>();

	// Store projection info - which columns DuckDB wants
	// Column 0 is always __document_id, so we adjust column IDs by -1
	// Note: input.column_ids may contain COLUMN_IDENTIFIER_ROW_ID for rowid requests
	bind_data.projected_columns.clear();
	for (auto col_id : input.column_ids) {
		if (col_id == COLUMN_IDENTIFIER_ROW_ID) {
			// DuckDB is requesting rowid - map to our __document_id
			bind_data.projected_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
		} else if (col_id == 0) {
			// __document_id column (our column 0)
			bind_data.projected_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
		} else {
			// Regular column - adjust for __document_id offset
			bind_data.projected_columns.push_back(col_id - 1);
		}
	}

	auto global_state = make_uniq<FirestoreScanGlobalState>();

	// Document path mode: fetch all subcollection IDs, sort, then truncate to limit.
	if (bind_data.is_document_path) {
		global_state->is_document_path = true;
		global_state->client = make_uniq<FirestoreClient>(bind_data.credentials);

		auto order = bind_data.docpath_named_order;
		// Only apply limit at scan level when we also control ordering.
		// If order is NONE, DuckDB may need all rows for a client-side sort+limit.
		std::optional<int64_t> effective_docpath_limit;
		if (order != DocPathOrderType::NONE) {
			effective_docpath_limit = bind_data.limit.has_value() ? bind_data.limit : bind_data.sql_pushed_limit;
		}

		// Clear SQL pushdown fields — docpath scans are not Firestore document queries.
		bind_data.sql_pushed_order_by.clear();
		bind_data.sql_pushed_limit.reset();

		try {
			global_state->docpath_ids = FetchDocPathIds(*global_state->client, bind_data.collection);
			if (order != DocPathOrderType::NONE) {
				SortDocPathIds(global_state->docpath_ids, order);
			}
			if (effective_docpath_limit.has_value() && effective_docpath_limit.value() >= 0) {
				auto lim = static_cast<size_t>(effective_docpath_limit.value());
				if (global_state->docpath_ids.size() > lim) {
					global_state->docpath_ids.resize(lim);
				}
			}
		} catch (...) {
			global_state->docpath_ids.clear();
		}
		global_state->current_index = 0;
		global_state->finished = global_state->docpath_ids.empty();
		FS_LOG_DEBUG("Document path '" + bind_data.collection + "' has " +
		             std::to_string(global_state->docpath_ids.size()) + " subcollections");
		return std::move(global_state);
	}

	global_state->client = make_uniq<FirestoreClient>(bind_data.credentials);

	// Check if this is a collection group query (starts with ~)
	if (!bind_data.collection.empty() && bind_data.collection[0] == '~') {
		bind_data.is_collection_group = true;
	}

	// Process filter pushdown using candidate filters from pushdown_complex_filter callback
	if (!bind_data.candidate_pushdown_filters.empty() && bind_data.index_cache &&
	    bind_data.index_cache->fetch_succeeded) {
		global_state->pushdown_result = MatchFiltersToIndexes(bind_data.candidate_pushdown_filters,
		                                                      *bind_data.index_cache, bind_data.is_collection_group);
	}

	// Compute effective ORDER BY and LIMIT values.
	// Named parameters (order_by, scan_limit) take precedence over SQL-pushed values.
	auto &effective_order_by =
	    !bind_data.parsed_order_by.empty() ? bind_data.parsed_order_by : bind_data.sql_pushed_order_by;
	auto effective_limit = bind_data.limit.has_value() ? bind_data.limit : bind_data.sql_pushed_limit;

	// Build query and fetch initial documents
	FirestoreListResponse response;

	if (global_state->pushdown_result.has_pushdown()) {
		// Build StructuredQuery with WHERE clause
		std::string collection_id = bind_data.collection;
		if (bind_data.is_collection_group && !collection_id.empty() && collection_id[0] == '~') {
			collection_id = collection_id.substr(1);
		}

		json sq;
		sq["from"] = {{{"collectionId", collection_id}, {"allDescendants", bind_data.is_collection_group}}};

		// Add WHERE clause
		sq["where"] = BuildWhereClause(global_state->pushdown_result.pushed_filters);

		// Add orderBy - Firestore requires inequality/range filter fields to appear
		// before __name__ in the orderBy clause
		bool can_order = CanOrderByOnServer(effective_order_by, bind_data);
		FS_LOG_DEBUG("InitGlobal: CanOrderByOnServer=" + std::to_string(can_order));

		// If SQL ORDER BY was pushed but can't be sent to the server (no index),
		// we must also clear the SQL pushed limit. Otherwise Firestore returns a
		// limited subset in default order, and DuckDB re-sorts only that subset,
		// producing wrong results.
		if (!can_order && !bind_data.sql_pushed_order_by.empty() && bind_data.sql_pushed_limit.has_value()) {
			FS_LOG_DEBUG("InitGlobal: clearing SQL pushed limit because ORDER BY can't be sent to server");
			bind_data.sql_pushed_limit.reset();
			effective_limit = bind_data.limit.has_value() ? bind_data.limit : bind_data.sql_pushed_limit;
		}

		// Add limit
		int64_t page_size = 1000;
		if (effective_limit.has_value()) {
			page_size = std::min(effective_limit.value(), static_cast<int64_t>(1000));
		}
		sq["limit"] = page_size;

		if (can_order) {
			json order_by_arr = json::array();
			for (auto &ob : effective_order_by) {
				order_by_arr.push_back({{"field", {{"fieldPath", ob.field_path}}}, {"direction", ob.direction}});
			}
			// Append __name__ for cursor-based pagination if not already present.
			// Use the last field's direction so all orderBy fields are consistent —
			// Firestore requires matching directions unless a composite index exists.
			bool has_name = false;
			for (auto &ob : effective_order_by) {
				if (ob.field_path == "__name__") {
					has_name = true;
					break;
				}
			}
			if (!has_name) {
				std::string name_direction = effective_order_by.back().direction;
				order_by_arr.push_back({{"field", {{"fieldPath", "__name__"}}}, {"direction", name_direction}});
			}
			sq["orderBy"] = order_by_arr;
		} else if (!effective_order_by.empty()) {
			FS_LOG_DEBUG("Skipping server-side order_by in pushdown query (no supporting index)");
		}

		if (!sq.contains("orderBy")) {
			// Build orderBy: inequality/range fields first, then __name__ last.
			// Firestore requires inequality filter fields (range, NOT_EQUAL, NOT_IN,
			// IS_NOT_NULL) to appear in orderBy before __name__.
			json order_by_arr = json::array();
			std::set<std::string> added_fields;

			for (auto &f : global_state->pushdown_result.pushed_filters) {
				// Firestore inequality-like operators that require orderBy on the field:
				// - Range: LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL
				// - NOT_EQUAL, NOT_IN
				// - IS_NOT_NULL (unary)
				// Equality operators (EQUAL, IN) do NOT require orderBy.
				bool needs_order = false;
				if (f.is_unary && f.unary_op == "IS_NOT_NULL") {
					needs_order = true;
				} else if (!f.is_unary && !f.is_in_filter) {
					// Check if it's a range or NOT_EQUAL op
					if (f.firestore_op == "LESS_THAN" || f.firestore_op == "LESS_THAN_OR_EQUAL" ||
					    f.firestore_op == "GREATER_THAN" || f.firestore_op == "GREATER_THAN_OR_EQUAL" ||
					    f.firestore_op == "NOT_EQUAL") {
						needs_order = true;
					}
				} else if (f.is_in_filter && f.firestore_op == "NOT_IN") {
					needs_order = true;
				}

				if (needs_order && added_fields.find(f.field_path) == added_fields.end()) {
					order_by_arr.push_back({{"field", {{"fieldPath", f.field_path}}}, {"direction", "ASCENDING"}});
					added_fields.insert(f.field_path);
				}
			}

			// __name__ always last for cursor-based pagination
			order_by_arr.push_back({{"field", {{"fieldPath", "__name__"}}}, {"direction", "ASCENDING"}});

			sq["orderBy"] = order_by_arr;
		}

		global_state->structured_query = sq;
		global_state->uses_run_query = true;
		global_state->query_page_size = page_size;

		FS_LOG_DEBUG("Filter pushdown active: " + std::to_string(global_state->pushdown_result.pushed_filters.size()) +
		             " filters pushed to Firestore");
		FS_LOG_DEBUG("InitGlobal: structured_query=" + sq.dump());

		try {
			response = global_state->client->RunQuery(bind_data.collection, sq, bind_data.is_collection_group);
			global_state->next_page_token = ""; // runQuery uses cursor-based pagination
			// Check if this page is full (may need pagination) or partial (we're done)
			global_state->last_page_was_full =
			    (static_cast<int64_t>(response.documents.size()) >= global_state->query_page_size);
			FS_LOG_DEBUG("InitGlobal: RunQuery returned " + std::to_string(response.documents.size()) + " documents");
		} catch (const std::exception &e) {
			// Fallback: if runQuery fails, disable pushdown and use ListDocuments
			FS_LOG_WARN("RunQuery with filters failed, falling back to full scan: " + std::string(e.what()));
			global_state->pushdown_result = FirestoreFilterResult {};
			global_state->uses_run_query = false;
			global_state->structured_query = {};
			global_state->pushdown_failed = true;

			FirestoreQuery query;
			query.show_missing = bind_data.show_missing;
			// Note: do NOT apply scan_limit to page_size here. The filters that were
			// supposed to run on Firestore will now run client-side in DuckDB, so we
			// need to fetch full pages to avoid missing matching documents.
			// The scan_limit is still enforced in FirestoreScanFunction after DuckDB filtering.
			if (CanOrderByOnServer(effective_order_by, bind_data)) {
				query.order_by = FormatOrderByForREST(effective_order_by);
			} else if (!effective_order_by.empty()) {
				FS_LOG_DEBUG("Skipping server-side order_by in fallback (no supporting index)");
			}

			if (bind_data.is_collection_group) {
				std::string coll_id = bind_data.collection.substr(1);
				response = global_state->client->CollectionGroupQuery(coll_id, query);
				global_state->next_page_token = "";
			} else {
				response = global_state->client->ListDocuments(bind_data.collection, query);
				global_state->next_page_token = response.next_page_token;
			}
		}
	} else {
		// No filter pushdown - use existing query paths
		FirestoreQuery query;
		query.show_missing = bind_data.show_missing;

		bool can_order_fallback = CanOrderByOnServer(effective_order_by, bind_data);

		// If SQL ORDER BY was pushed but can't be sent to server, clear SQL pushed limit
		if (!can_order_fallback && !bind_data.sql_pushed_order_by.empty() && bind_data.sql_pushed_limit.has_value()) {
			FS_LOG_DEBUG(
			    "InitGlobal (no-pushdown): clearing SQL pushed limit because ORDER BY can't be sent to server");
			bind_data.sql_pushed_limit.reset();
			effective_limit = bind_data.limit.has_value() ? bind_data.limit : bind_data.sql_pushed_limit;
		}

		// Only apply scan_limit to page_size when there are no unpushed filters.
		// If DuckDB will filter client-side, we need full pages to avoid missing matches.
		bool unpushed = !bind_data.candidate_pushdown_filters.empty();
		if (effective_limit.has_value() && !unpushed) {
			query.page_size = std::min(effective_limit.value(), static_cast<int64_t>(1000));
		}
		if (can_order_fallback) {
			query.order_by = FormatOrderByForREST(effective_order_by);
		} else if (!effective_order_by.empty()) {
			FS_LOG_DEBUG("Skipping server-side order_by (no supporting index); DuckDB will sort client-side");
		}

		if (bind_data.is_collection_group) {
			std::string collection_id = bind_data.collection.substr(1);
			response = global_state->client->CollectionGroupQuery(collection_id, query);
			global_state->next_page_token = "";
		} else {
			response = global_state->client->ListDocuments(bind_data.collection, query);
			global_state->next_page_token = response.next_page_token;
		}
	}

	global_state->documents = std::move(response.documents);
	global_state->current_index = 0;
	global_state->finished = global_state->documents.empty();

	return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> FirestoreScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	return make_uniq<FirestoreScanLocalState>();
}

void FirestoreScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<FirestoreScanBindData>();
	auto &global_state = data.global_state->Cast<FirestoreScanGlobalState>();
	bool is_document_path_mode = global_state.is_document_path;

	if (global_state.finished) {
		output.SetCardinality(0);
		return;
	}

	if (is_document_path_mode) {
		fprintf(stderr, "[DEBUG-SCAN-DOCPATH] available=%llu current_index=%llu\n",
		        static_cast<unsigned long long>(global_state.docpath_ids.size()),
		        static_cast<unsigned long long>(global_state.current_index));
		fflush(stderr);
		idx_t available = global_state.docpath_ids.size();
		if (global_state.current_index >= available) {
			global_state.finished = true;
			output.SetCardinality(0);
			return;
		}

		idx_t count = 0;
		idx_t max_count = std::min<idx_t>(STANDARD_VECTOR_SIZE, available - global_state.current_index);
		auto out_data = FlatVector::GetData<string_t>(output.data[0]);

		while (count < max_count) {
			const auto &doc_id = global_state.docpath_ids[global_state.current_index++];
			out_data[count++] = StringVector::AddString(output.data[0], doc_id);
		}

		if (global_state.current_index >= available) {
			global_state.finished = true;
		}
		output.SetCardinality(count);
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	// Apply effective limit (named param takes precedence over SQL-pushed limit).
	// Skip when DuckDB will filter client-side: the scan emits rows before DuckDB's FILTER
	// node, so cutting off here would drop rows that might match the WHERE clause.
	// This happens when: (a) pushdown was attempted but failed at runtime, or
	// (b) candidate filters exist but none were pushed (e.g. collection group without indexes).
	auto effective_limit = bind_data.limit;
	if (!effective_limit.has_value() && !is_document_path_mode) {
		effective_limit = bind_data.sql_pushed_limit;
	}
	bool has_unpushed_filters =
	    !bind_data.candidate_pushdown_filters.empty() && !global_state.pushdown_result.has_pushdown();
	if (effective_limit.has_value() && !global_state.pushdown_failed && !has_unpushed_filters) {
		idx_t total_returned = global_state.current_index;
		if (total_returned >= static_cast<idx_t>(effective_limit.value())) {
			global_state.finished = true;
			output.SetCardinality(0);
			return;
		}
		max_count = std::min(max_count, static_cast<idx_t>(effective_limit.value()) - total_returned);
	}

	while (count < max_count) {
		// Check if we need to fetch more documents
		if (global_state.current_index >= global_state.documents.size()) {
			if (global_state.uses_run_query) {
				// Cursor-based pagination for :runQuery
				// If the last page wasn't full, we've reached the end - no need to fetch more
				if (!global_state.last_page_was_full || global_state.documents.empty()) {
					break;
				}

				// Build cursor values from last document for all orderBy fields
				auto &last_doc = global_state.documents.back();
				json cursor_values = json::array();

				if (global_state.structured_query.contains("orderBy")) {
					for (auto &ob_entry : global_state.structured_query["orderBy"]) {
						std::string fp = ob_entry["field"]["fieldPath"].get<std::string>();
						if (fp == "__name__") {
							cursor_values.push_back({{"referenceValue", last_doc.name}});
						} else if (last_doc.fields.contains(fp)) {
							cursor_values.push_back(last_doc.fields[fp]);
						} else {
							cursor_values.push_back({{"nullValue", nullptr}});
						}
					}
				} else {
					cursor_values.push_back({{"referenceValue", last_doc.name}});
				}

				// Update the structured query with startAt cursor
				json paginated_query = global_state.structured_query;
				paginated_query["startAt"] = {{"values", cursor_values}, {"before", false}};

				auto response =
				    global_state.client->RunQuery(bind_data.collection, paginated_query, bind_data.is_collection_group);

				// Track if this page is full for next iteration
				global_state.last_page_was_full =
				    (static_cast<int64_t>(response.documents.size()) >= global_state.query_page_size);

				if (response.documents.empty()) {
					break;
				}

				global_state.documents = std::move(response.documents);
				global_state.current_index = 0;
			} else {
				// Page token-based pagination for ListDocuments
				if (global_state.next_page_token.empty()) {
					break;
				}

				FirestoreQuery query;
				query.show_missing = bind_data.show_missing;
				query.page_token = global_state.next_page_token;
				bool can_order_named =
				    !bind_data.parsed_order_by.empty() && CanOrderByOnServer(bind_data.parsed_order_by, bind_data);
				if (can_order_named) {
					query.order_by = FormatOrderByForREST(bind_data.parsed_order_by);
				}

				auto response = global_state.client->ListDocuments(bind_data.collection, query);

				if (response.documents.empty()) {
					break;
				}

				global_state.documents = std::move(response.documents);
				global_state.next_page_token = response.next_page_token;
				global_state.current_index = 0;
			}
		}

		auto &doc = global_state.documents[global_state.current_index];

		// Set values for each projected column
		for (idx_t out_col = 0; out_col < bind_data.projected_columns.size(); out_col++) {
			idx_t src_col = bind_data.projected_columns[out_col];

			if (src_col == COLUMN_IDENTIFIER_ROW_ID) {
				// __document_id column
				std::string doc_id;
				if (bind_data.is_collection_group) {
					// For collection group queries, use the full document path
					// to uniquely identify documents across different parent collections
					// doc.name is like: projects/{PROJECT}/databases/{DB}/documents/{PATH}
					// We extract just the {PATH} part
					const std::string marker = "/documents/";
					size_t pos = doc.name.find(marker);
					if (pos != std::string::npos) {
						doc_id = doc.name.substr(pos + marker.length());
					} else {
						// Fallback to full name if marker not found
						doc_id = doc.name;
					}
				} else {
					// For regular queries, use just the document ID
					doc_id = doc.document_id;
				}
				FlatVector::GetData<string_t>(output.data[out_col])[count] =
				    StringVector::AddString(output.data[out_col], doc_id);
			} else {
				// Regular field column
				const auto &col_name = bind_data.column_names[src_col];

				if (doc.fields.contains(col_name)) {
					SetDuckDBValue(output.data[out_col], count, doc.fields[col_name], bind_data.column_types[src_col]);
				} else {
					FlatVector::SetNull(output.data[out_col], count, true);
				}
			}
		}

		count++;
		global_state.current_index++;
	}

	if (count == 0) {
		global_state.finished = true;
	} else if (global_state.current_index >= global_state.documents.size()) {
		if (global_state.uses_run_query) {
			// For runQuery, we know we're done when a fetch returns fewer docs than the limit
			// The next pagination attempt in the loop above will confirm with an empty response
			// Don't mark finished yet - let the next iteration's fetch determine it
		} else if (global_state.next_page_token.empty()) {
			global_state.finished = true;
		}
	}

	output.SetCardinality(count);
}

} // namespace duckdb
