#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "firestore_client.hpp"
#include "firestore_schema_accumulator.hpp"
#include "firestore_index.hpp"
#include "firestore_paging.hpp"
#include <memory>
#include <mutex>
#include <set>
#include <vector>

namespace duckdb {

class ExtensionLoader;

enum class DocPathOrderType : uint8_t { NONE = 0, ASCENDING, DESCENDING };

// Bind data - stores parameters from SQL call
struct FirestoreScanBindData : public TableFunctionData {
	std::string collection;
	std::shared_ptr<FirestoreCredentials> credentials;
	std::vector<std::string> column_names; // All column names (excluding __document_id)
	std::vector<LogicalType> column_types; // All column types (excluding __document_id)

	// Projection info - which columns to actually output
	std::vector<idx_t> projected_columns; // Indices into column_names/column_types

	// Set by the optimizer extension when this scan feeds nothing but a bare
	// COUNT(*): the scan then has to produce the right *number* of rows, and
	// nothing may read their values.
	//
	// The projection cannot reveal this. DuckDB does not ask a table function
	// for zero columns; for count(*) it projects the first column, which here
	// is __document_id -- indistinguishable from someone genuinely selecting
	// it. Only the plan shape tells them apart.
	bool count_star_only = false;

	// Query options
	std::optional<int64_t> limit;

	// Documents requested per round trip. Unset means "use the
	// firestore_page_size setting". Lower it for collections whose documents
	// are large enough that a full 1000-document page will not fit in memory.
	std::optional<int64_t> page_size;
	std::optional<std::string> order_by;
	std::vector<OrderByField> parsed_order_by; // Parsed from order_by string at bind time

	// Collection group query flag - when true, __document_id returns full path
	bool is_collection_group = false;

	// Show missing/phantom documents (documents with no fields, only subcollections)
	bool show_missing = true;

	// Whether a SQL ORDER BY may be sent to Firestore for this scan. Unset
	// follows the firestore_orderby_pushdown setting; see FirestoreSettings.
	// Setting it true also skips the safety check below -- an explicit request
	// for Firestore's ordering is taken at face value.
	std::optional<bool> orderby_pushdown;

	// Which fields the sampled documents say Firestore may be asked to sort
	// by. Null when no schema was inferred (an explicit `columns` override),
	// which is treated as "nothing is known to be safe".
	std::shared_ptr<FirestoreSchemaAccumulator::OrderingSafety> ordering_safety;

	// How mapValue fields are surfaced. Defaults to WIRE so existing queries
	// that reach into $.x.mapValue.fields.y keep working.
	FirestoreMapEncoding map_encoding = FirestoreMapEncoding::WIRE;

	// Documents sampled to infer the schema. Unset means "use the
	// firestore_schema_sample_size setting"; <= 0 means every document.
	std::optional<int64_t> schema_sample_size;

	// Append a trailing __unmapped column carrying fields that are not in the
	// schema, so data missed by sampling stays reachable.
	bool unmapped_column = false;

	// Explicit schema supplied via columns:={...}; when set, inference is
	// skipped entirely (which also avoids the bind-time sampling request).
	bool has_columns_override = false;

	// Sorted copy of column_names, used to spot fields a document carries but
	// the schema does not. Sorted so it can be merged against a document's
	// fields in one linear pass (nlohmann objects iterate in key order).
	std::vector<std::string> sorted_known_columns;

	// Document path mode: when the path has even segments (e.g. "artifacts/default-app-id"),
	// we list subcollections during execution and return them as virtual __document_id rows.
	bool is_document_path = false;

	// Document-path ordering direction (from named param or SQL ORDER BY).
	DocPathOrderType docpath_named_order = DocPathOrderType::NONE;

	// Index cache - populated at bind time for filter pushdown
	std::shared_ptr<FirestoreIndexCache> index_cache;

	// Candidate filters extracted by pushdown_complex_filter callback
	// These are stored here so InitGlobal can use them to build the Firestore query.
	// All original DuckDB expressions are left intact so DuckDB re-verifies results.
	std::vector<FirestorePushdownFilter> candidate_pushdown_filters;

	// SQL pushdown: ORDER BY / LIMIT extracted from the logical plan by the optimizer extension.
	// Named parameters (order_by, scan_limit) take precedence over these when both are set.
	// The original SQL ORDER BY / LIMIT nodes are left in place so DuckDB re-verifies results.
	std::vector<OrderByField> sql_pushed_order_by;
	std::optional<int64_t> sql_pushed_limit;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<FirestoreScanBindData>();
		*copy = *this;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<FirestoreScanBindData>();

		bool credentials_equal = credentials == other.credentials;
		if (!credentials_equal && credentials && other.credentials) {
			credentials_equal = credentials->type == other.credentials->type &&
			                    credentials->project_id == other.credentials->project_id &&
			                    credentials->database_id == other.credentials->database_id &&
			                    credentials->client_email == other.credentials->client_email &&
			                    credentials->api_key == other.credentials->api_key;
		}

		auto order_fields_equal = [](const std::vector<OrderByField> &left,
		                             const std::vector<OrderByField> &right) -> bool {
			if (left.size() != right.size()) {
				return false;
			}
			for (idx_t i = 0; i < left.size(); i++) {
				if (left[i].field_path != right[i].field_path || left[i].direction != right[i].direction) {
					return false;
				}
			}
			return true;
		};

		return collection == other.collection && column_names == other.column_names &&
		       column_types == other.column_types && projected_columns == other.projected_columns &&
		       limit == other.limit && order_by == other.order_by &&
		       order_fields_equal(parsed_order_by, other.parsed_order_by) &&
		       is_collection_group == other.is_collection_group && show_missing == other.show_missing &&
		       is_document_path == other.is_document_path && docpath_named_order == other.docpath_named_order &&
		       credentials_equal && order_fields_equal(sql_pushed_order_by, other.sql_pushed_order_by) &&
		       sql_pushed_limit == other.sql_pushed_limit && page_size == other.page_size;
	}
};

// Global state - shared across threads
struct FirestoreScanGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	std::vector<FirestoreDocument> documents;
	std::vector<std::string> docpath_ids;
	bool is_document_path = false;
	idx_t current_index;
	bool finished;
	std::string next_page_token;

	// Field names already reported as unmapped, so a long scan warns once per
	// distinct field rather than once per row.
	std::set<std::string> reported_unmapped;

	// Running count of rows handed to DuckDB across the whole scan.
	// `current_index` cannot serve this purpose: it indexes into the *current
	// page* and is reset to 0 every time a new page is fetched, so any
	// scan_limit larger than one page would never be reached.
	idx_t rows_emitted = 0;

	// Filter pushdown state
	FirestoreFilterResult pushdown_result;
	json structured_query;       // Cached StructuredQuery for pagination
	bool uses_run_query = false; // Whether using :runQuery (true when filters pushed)

	// When true, filter pushdown failed and DuckDB filters client-side.
	// scan_limit must NOT be enforced in FirestoreScanFunction because
	// it would cut off rows before DuckDB's FILTER node runs.
	bool pushdown_failed = false;

	// Chooses the page size for each round trip and shrinks it when a page
	// weighs more than the byte budget allows.
	FirestorePageSizePolicy page_policy;

	// Fields Firestore is asked to return, derived from DuckDB's projection.
	// Unselected fields then never cross the wire.
	FirestoreProjection projection;

	// Rows still to emit when the scan was answered by a count rather than by
	// fetching documents. Unset means this is an ordinary scan.
	std::optional<int64_t> counted_rows_remaining;

	// Documents the *last* request actually asked for. The end-of-results
	// check compares against this rather than the policy's current size:
	// after a shrink those differ, and comparing against the new (smaller)
	// size would read a full page as short and end the scan early.
	int64_t query_page_size = FIRESTORE_DEFAULT_PAGE_SIZE;
	bool last_page_was_full = true; // Whether last fetch returned a full page

	FirestoreScanGlobalState() : current_index(0), finished(false) {
	}

	// ---- parallel scanning -------------------------------------------------
	//
	// A collection can be read by several threads at once by cutting its
	// document-name space into ranges and giving each thread a range to page
	// through. Firestore's REST pagination is sequential *within* a range, but
	// the ranges are independent, so the round trips overlap.
	//
	// There are more ranges than threads: key distributions are rarely even,
	// and a thread that finishes a light range takes the next one rather than
	// idling while another grinds through a heavy one.

	// Ranges not yet claimed. Empty when the scan is sequential.
	std::vector<FirestoreKeyRange> key_range_partitions;
	idx_t next_partition = 0;
	std::mutex partition_mutex;

	// Collection identity in the form range cursors need:
	// projects/P/databases/D/documents/<collection>, and the final segment.
	std::string document_path_prefix;
	std::string collection_id;

	// Threads to ask DuckDB for. 1 means this scan runs sequentially.
	idx_t scan_threads = 1;

	// Guards reported_unmapped, which every thread adds to.
	std::mutex unmapped_mutex;

	idx_t MaxThreads() const override {
		return scan_threads;
	}
};

// Local state - one per thread.
//
// In a parallel scan each thread owns its key range, its HTTP client (httplib
// clients are not shareable across threads), its page of documents and its own
// paging policy. In a sequential scan none of this is used and the state below
// stays empty.
struct FirestoreScanLocalState : public LocalTableFunctionState {
	std::unique_ptr<FirestoreClient> client;
	std::vector<FirestoreDocument> documents;
	idx_t current_index = 0;

	bool has_partition = false;
	FirestoreKeyRange partition;
	json structured_query;
	bool last_page_was_full = true;
	FirestorePageSizePolicy page_policy;
};

// Register the firestore_scan function
void RegisterFirestoreScanFunction(ExtensionLoader &loader);

// Table function callbacks
unique_ptr<FunctionData> FirestoreScanBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> FirestoreScanInitGlobal(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> FirestoreScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state);

void FirestoreScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

// Clear the schema cache (useful when schema changes or for testing)
// If collection is empty, clears entire cache. Otherwise clears only entries for that collection.
void ClearFirestoreSchemaCache(const std::string &collection = "");

} // namespace duckdb
