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
#include <chrono>

namespace duckdb {

// Cached schema entry with timestamp
struct CachedSchemaEntry {
	std::vector<std::pair<std::string, LogicalType>> schema;
	std::shared_ptr<FirestoreIndexCache> index_cache;
	std::chrono::steady_clock::time_point cached_at;

	bool IsExpired() const {
		int64_t ttl = FirestoreSettings::SchemaCacheTTLSeconds();
		if (ttl == 0) {
			return true; // Cache disabled
		}
		auto age = std::chrono::steady_clock::now() - cached_at;
		return age > std::chrono::seconds(ttl);
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
		// Clear entries that match the collection (cache key format is "project_id:collection")
		auto it = schema_cache.begin();
		int cleared = 0;
		while (it != schema_cache.end()) {
			// Check if the cache key ends with ":collection"
			const std::string &key = it->first;
			size_t colon_pos = key.find(':');
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
			get.extra_info.file_filters = "Firestore Pushed Filters: " + info;
		}
	}

	// IMPORTANT: Do NOT remove any expressions from the filters vector.
	// By leaving them all in place, DuckDB will apply them as post-scan filters,
	// ensuring correctness regardless of Firestore's filtering semantics.
}

void RegisterFirestoreScanFunction(ExtensionLoader &loader) {
	TableFunction scan_func("firestore_scan", {LogicalType::VARCHAR}, // collection name (required)
	                        FirestoreScanFunction, FirestoreScanBind, FirestoreScanInitGlobal, FirestoreScanInitLocal);

	// Add named parameters
	scan_func.named_parameters["project_id"] = LogicalType::VARCHAR;
	scan_func.named_parameters["credentials"] = LogicalType::VARCHAR;
	scan_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	scan_func.named_parameters["database"] = LogicalType::VARCHAR;
	scan_func.named_parameters["limit"] = LogicalType::BIGINT;
	scan_func.named_parameters["order_by"] = LogicalType::VARCHAR;

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
		} else if (kv.first == "limit") {
			result->limit = kv.second.GetValue<int64_t>();
		} else if (kv.first == "order_by") {
			result->order_by = kv.second.GetValue<string>();
		}
	}

	// Resolve credentials from various sources
	result->credentials = ResolveFirestoreCredentials(context, project_id, credentials_path, api_key, database_id);

	if (!result->credentials) {
		throw BinderException(
		    "No Firestore credentials found. Provide credentials parameter, "
		    "create a secret with CREATE SECRET, or set GOOGLE_APPLICATION_CREDENTIALS environment variable.");
	}

	std::string cache_key = result->credentials->project_id + ":" + result->collection;
	{
		std::lock_guard<std::mutex> lock(schema_cache_mutex);
		auto it = schema_cache.find(cache_key);
		if (it != schema_cache.end() && !it->second.IsExpired()) {
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
	auto schema = client.InferSchema(result->collection);

	// Check if collection exists (has documents)
	if (schema.empty()) {
		bool is_collection_group = !result->collection.empty() && result->collection[0] == '~';
		std::string collection_type = is_collection_group ? "Collection group" : "Collection";
		std::string display_name = is_collection_group ? result->collection.substr(1) : result->collection;

		FirestoreErrorContext ctx;
		ctx.withCollection(result->collection).withProject(result->credentials->project_id).withOperation("scan");

		throw FirestoreNotFoundError(
		    FirestoreErrorCode::NOT_FOUND_COLLECTION,
		    collection_type + " '" + display_name + "' does not exist or contains no documents.", ctx);
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

		// Add limit
		int64_t page_size = 1000;
		if (bind_data.limit.has_value()) {
			page_size = std::min(bind_data.limit.value(), static_cast<int64_t>(1000));
		}
		sq["limit"] = page_size;

		// Add orderBy - Firestore requires inequality/range filter fields to appear
		// before __name__ in the orderBy clause
		if (bind_data.order_by.has_value()) {
			std::string order_str = bind_data.order_by.value();
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
			sq["orderBy"] = {{{"field", {{"fieldPath", field_name}}}, {"direction", direction}}};
		} else {
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

		try {
			response = global_state->client->RunQuery(bind_data.collection, sq, bind_data.is_collection_group);
			global_state->next_page_token = ""; // runQuery uses cursor-based pagination
			// Check if this page is full (may need pagination) or partial (we're done)
			global_state->last_page_was_full =
			    (static_cast<int64_t>(response.documents.size()) >= global_state->query_page_size);
		} catch (const std::exception &e) {
			// Fallback: if runQuery fails, disable pushdown and use ListDocuments
			FS_LOG_WARN("RunQuery with filters failed, falling back to full scan: " + std::string(e.what()));
			global_state->pushdown_result = FirestoreFilterResult {};
			global_state->uses_run_query = false;
			global_state->structured_query = {};

			FirestoreQuery query;
			if (bind_data.limit.has_value()) {
				query.page_size = std::min(bind_data.limit.value(), static_cast<int64_t>(1000));
			}
			if (bind_data.order_by.has_value()) {
				query.order_by = bind_data.order_by.value();
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
		if (bind_data.limit.has_value()) {
			query.page_size = std::min(bind_data.limit.value(), static_cast<int64_t>(1000));
		}
		if (bind_data.order_by.has_value()) {
			query.order_by = bind_data.order_by.value();
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

	if (global_state.finished) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	// Apply limit if specified
	if (bind_data.limit.has_value()) {
		idx_t total_returned = global_state.current_index;
		if (total_returned >= static_cast<idx_t>(bind_data.limit.value())) {
			global_state.finished = true;
			output.SetCardinality(0);
			return;
		}
		max_count = std::min(max_count, static_cast<idx_t>(bind_data.limit.value()) - total_returned);
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

				// Use the last document's __name__ as cursor for startAfter
				auto &last_doc = global_state.documents.back();
				json cursor_value = {{"referenceValue", last_doc.name}};

				// Update the structured query with startAt cursor
				json paginated_query = global_state.structured_query;
				paginated_query["startAt"] = {{"values", {cursor_value}}, {"before", false}};

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
				query.page_token = global_state.next_page_token;
				if (bind_data.order_by.has_value()) {
					query.order_by = bind_data.order_by.value();
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
