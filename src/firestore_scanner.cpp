#include "firestore_scanner.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

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
static void FirestoreComplexFilterPushdown(ClientContext &context, LogicalGet &get,
                                           FunctionData *bind_data_p,
                                           vector<unique_ptr<Expression>> &filters) {
    auto &bind_data = bind_data_p->Cast<FirestoreScanBindData>();

    if (!bind_data.index_cache || !bind_data.index_cache->fetch_succeeded) {
        return;
    }

    bind_data.candidate_pushdown_filters.clear();

    for (auto &filter : filters) {
        auto converted = ConvertExpressionToFilters(
            *filter, get.table_index, bind_data.column_names, bind_data.column_types);
        bind_data.candidate_pushdown_filters.insert(
            bind_data.candidate_pushdown_filters.end(),
            std::make_move_iterator(converted.begin()),
            std::make_move_iterator(converted.end()));
    }

    // Match against indexes now to populate EXPLAIN output
    if (!bind_data.candidate_pushdown_filters.empty()) {
        bool is_collection_group = !bind_data.collection.empty() && bind_data.collection[0] == '~';
        auto result = MatchFiltersToIndexes(
            bind_data.candidate_pushdown_filters, *bind_data.index_cache, is_collection_group);

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
    TableFunction scan_func("firestore_scan",
                            {LogicalType::VARCHAR},  // collection name (required)
                            FirestoreScanFunction,
                            FirestoreScanBind,
                            FirestoreScanInitGlobal,
                            FirestoreScanInitLocal);

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

unique_ptr<FunctionData> FirestoreScanBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
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
    result->credentials = ResolveFirestoreCredentials(
        context, project_id, credentials_path, api_key, database_id);

    if (!result->credentials) {
        throw BinderException("No Firestore credentials found. Provide credentials parameter, "
                             "create a secret with CREATE SECRET, or set GOOGLE_APPLICATION_CREDENTIALS environment variable.");
    }

    // Create client and infer schema from collection
    FirestoreClient client(result->credentials);
    auto schema = client.InferSchema(result->collection);

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
        FS_LOG_DEBUG("Index cache populated: " +
                     std::to_string(result->index_cache->composite_indexes.size()) +
                     " composite indexes, default_single_field=" +
                     (result->index_cache->default_single_field_enabled ? "true" : "false"));
    } catch (const std::exception &e) {
        FS_LOG_WARN("Failed to fetch indexes, filter pushdown disabled: " + std::string(e.what()));
        result->index_cache->fetch_succeeded = false;
    }

    return std::move(result);
}

unique_ptr<GlobalTableFunctionState> FirestoreScanInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input
) {
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
    if (!bind_data.candidate_pushdown_filters.empty() &&
        bind_data.index_cache && bind_data.index_cache->fetch_succeeded) {
        global_state->pushdown_result = MatchFiltersToIndexes(
            bind_data.candidate_pushdown_filters, *bind_data.index_cache, bind_data.is_collection_group);
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
        sq["from"] = {{
            {"collectionId", collection_id},
            {"allDescendants", bind_data.is_collection_group}
        }};

        // Add WHERE clause
        sq["where"] = BuildWhereClause(global_state->pushdown_result.pushed_filters);

        // Add limit
        int64_t page_size = 1000;
        if (bind_data.limit.has_value()) {
            page_size = std::min(bind_data.limit.value(), static_cast<int64_t>(1000));
        }
        sq["limit"] = page_size;

        // Add orderBy
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
            sq["orderBy"] = {{
                {"field", {{"fieldPath", field_name}}},
                {"direction", direction}
            }};
        } else {
            // Default orderBy __name__ for cursor-based pagination
            sq["orderBy"] = {{
                {"field", {{"fieldPath", "__name__"}}},
                {"direction", "ASCENDING"}
            }};
        }

        global_state->structured_query = sq;
        global_state->uses_run_query = true;

        FS_LOG_DEBUG("Filter pushdown active: " +
                     std::to_string(global_state->pushdown_result.pushed_filters.size()) +
                     " filters pushed to Firestore");

        try {
            response = global_state->client->RunQuery(
                bind_data.collection, sq, bind_data.is_collection_group);
            global_state->next_page_token = "";  // runQuery uses cursor-based pagination
        } catch (const std::exception &e) {
            // Fallback: if runQuery fails, disable pushdown and use ListDocuments
            FS_LOG_WARN("RunQuery with filters failed, falling back to full scan: " +
                        std::string(e.what()));
            global_state->pushdown_result = FirestoreFilterResult{};
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

unique_ptr<LocalTableFunctionState> FirestoreScanInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state
) {
    return make_uniq<FirestoreScanLocalState>();
}

void FirestoreScanFunction(
    ClientContext &context,
    TableFunctionInput &data,
    DataChunk &output
) {
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
        max_count = std::min(max_count,
            static_cast<idx_t>(bind_data.limit.value()) - total_returned);
    }

    while (count < max_count) {
        // Check if we need to fetch more documents
        if (global_state.current_index >= global_state.documents.size()) {
            if (global_state.uses_run_query) {
                // Cursor-based pagination for :runQuery
                if (global_state.documents.empty()) {
                    break;
                }

                // Use the last document's __name__ as cursor for startAfter
                auto &last_doc = global_state.documents.back();
                json cursor_value = {{"referenceValue", last_doc.name}};

                // Update the structured query with startAt cursor
                json paginated_query = global_state.structured_query;
                paginated_query["startAt"] = {
                    {"values", {cursor_value}},
                    {"before", false}
                };

                auto response = global_state.client->RunQuery(
                    bind_data.collection, paginated_query, bind_data.is_collection_group);

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

                auto response = global_state.client->ListDocuments(
                    bind_data.collection, query);

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
                    SetDuckDBValue(output.data[out_col], count,
                                  doc.fields[col_name],
                                  bind_data.column_types[src_col]);
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
