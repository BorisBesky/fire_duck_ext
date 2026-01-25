#include "firestore_scanner.hpp"
#include "firestore_types.hpp"
#include "firestore_secrets.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

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

    // Fetch initial batch of documents
    FirestoreQuery query;
    if (bind_data.limit.has_value()) {
        query.page_size = std::min(bind_data.limit.value(), static_cast<int64_t>(1000));
    }
    if (bind_data.order_by.has_value()) {
        query.order_by = bind_data.order_by.value();
    }

    FirestoreListResponse response;

    // Check if this is a collection group query (starts with ~)
    if (!bind_data.collection.empty() && bind_data.collection[0] == '~') {
        // Collection group query - set flag so __document_id returns full path
        bind_data.is_collection_group = true;
        std::string collection_id = bind_data.collection.substr(1);
        response = global_state->client->CollectionGroupQuery(collection_id, query);
        // Collection group queries don't support pagination in the same way
        global_state->next_page_token = "";
    } else {
        // Normal collection query
        response = global_state->client->ListDocuments(bind_data.collection, query);
        global_state->next_page_token = response.next_page_token;
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
            if (global_state.next_page_token.empty()) {
                // No more pages
                break;
            }

            // Fetch next page
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

    if (count == 0 ||
        (global_state.current_index >= global_state.documents.size() &&
         global_state.next_page_token.empty())) {
        global_state.finished = true;
    }

    output.SetCardinality(count);
}

} // namespace duckdb
