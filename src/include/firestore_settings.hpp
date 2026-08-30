#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "firestore_paging.hpp"

namespace duckdb {

struct FirestoreSettings {
	// Schema cache TTL in seconds (default: 3600 = 60 minutes, 0 = disabled)
	static constexpr int64_t kDefaultSchemaCacheTTLSeconds = 3600;

	static int64_t SchemaCacheTTLSeconds(const ClientContext &context) {
		Value ttl_value;
		if (context.TryGetCurrentSetting("firestore_schema_cache_ttl", ttl_value)) {
			return NormalizeTTL(ttl_value);
		}
		return kDefaultSchemaCacheTTLSeconds;
	}

	static void SetSchemaCacheTTLSeconds(ClientContext &context, SetScope scope, Value &parameter) {
		auto ttl = BigIntValue::Get(parameter);
		if (ttl < 0) {
			ttl = 0; // 0 means disabled
		}
		parameter = Value::BIGINT(ttl);
	}

	// Documents sampled to infer a collection's schema.
	// Firestore caps a single page at 1000, and inference already issues one
	// request, so 1000 is the deepest sample obtainable for one round trip.
	// -1 samples every document (paginating until the collection is exhausted).
	static constexpr int64_t kDefaultSchemaSampleSize = 1000;
	static constexpr int64_t kSampleAllDocuments = -1;

	static int64_t SchemaSampleSize(const ClientContext &context) {
		Value sample_value;
		if (context.TryGetCurrentSetting("firestore_schema_sample_size", sample_value)) {
			return NormalizeSampleSize(sample_value);
		}
		return kDefaultSchemaSampleSize;
	}

	static void SetSchemaSampleSize(ClientContext &context, SetScope scope, Value &parameter) {
		parameter = Value::BIGINT(NormalizeSampleSize(parameter));
	}

	// Documents requested per Firestore round trip.
	//
	// Firestore caps a page at 1000 and that is the right default, but a
	// collection of large documents can make a full page weigh more than the
	// process can hold: one document may be 1 MiB, so 1000 of them is ~1 GiB
	// of JSON before parsing. Lowering this trades round trips for a smaller
	// working set.
	static int64_t PageSize(const ClientContext &context) {
		Value page_size_value;
		if (context.TryGetCurrentSetting("firestore_page_size", page_size_value)) {
			return ClampFirestorePageSize(BigIntValue::Get(page_size_value));
		}
		return FIRESTORE_DEFAULT_PAGE_SIZE;
	}

	static void SetPageSize(ClientContext &context, SetScope scope, Value &parameter) {
		parameter = Value::BIGINT(ClampFirestorePageSize(BigIntValue::Get(parameter)));
	}

	// Ceiling, in uncompressed bytes, on what one page may weigh before the
	// scan asks for fewer documents per request. 0 disables the guard, leaving
	// the page size exactly where it was configured.
	static int64_t PageByteBudget(const ClientContext &context) {
		Value budget_value;
		if (context.TryGetCurrentSetting("firestore_page_byte_budget", budget_value)) {
			return NormalizeFirestorePageByteBudget(BigIntValue::Get(budget_value));
		}
		return FIRESTORE_DEFAULT_PAGE_BYTE_BUDGET;
	}

	static void SetPageByteBudget(ClientContext &context, SetScope scope, Value &parameter) {
		parameter = Value::BIGINT(NormalizeFirestorePageByteBudget(BigIntValue::Get(parameter)));
	}

	// Upper bound on threads a single scan may use.
	//
	// Firestore's REST pagination is sequential within a key range, so the only
	// way to overlap round trips is to read several ranges at once.
	//
	// Off by default, because whether that helps depends entirely on how the
	// collection's document ids are distributed, and the extension cannot know
	// that without paying to find out. Measured over 20,000 documents on
	// loopback: Firestore auto-ids, which spread evenly over the key space, go
	// from 1.08s to 0.67s on four threads; sequential ids, which all fall in
	// one range, go from 1.02s to 2.05s -- every range but one comes back
	// empty, the work funnels through a single thread anyway, and runQuery's
	// per-document envelope moves about 22% more bytes than documents.list.
	// Hand-chosen ids are common enough in Firestore that defaulting this on
	// would regress more collections than it helped.
	static constexpr int64_t kDefaultMaxThreads = 1;

	static int64_t MaxScanThreads(const ClientContext &context) {
		Value threads_value;
		if (context.TryGetCurrentSetting("firestore_max_threads", threads_value)) {
			return NormalizeMaxThreads(threads_value);
		}
		return kDefaultMaxThreads;
	}

	static void SetMaxScanThreads(ClientContext &context, SetScope scope, Value &parameter) {
		parameter = Value::BIGINT(NormalizeMaxThreads(parameter));
	}

	// Whether a SQL ORDER BY may be sent to Firestore.
	//
	// Firestore's ordering is not SQL's, so pushing the sort down changes what
	// a query returns rather than only how it is arranged. Firestore omits
	// documents that lack the ordering field, sorts null below every other
	// value, and orders across types by its own precedence -- while a field
	// holding more than one type reaches DuckDB as VARCHAR and compares as a
	// string. With a LIMIT the two disagree about which rows survive.
	//
	// Off by default, so `ORDER BY` means what SQL says it means and DuckDB
	// does the sorting. Turn it on to get Firestore's ordering, and the round
	// trips it saves, where the collection's shape makes them equivalent --
	// every document carrying the field, with one type.
	//
	// The named `order_by:=` parameter is unaffected: asking for the server's
	// ordering explicitly is already a choice, and this setting is about
	// whether an ordinary SQL query is quietly given those semantics.
	static constexpr bool kDefaultOrderByPushdown = false;

	static bool OrderByPushdown(const ClientContext &context) {
		Value pushdown_value;
		if (context.TryGetCurrentSetting("firestore_orderby_pushdown", pushdown_value)) {
			return !pushdown_value.IsNull() && BooleanValue::Get(pushdown_value);
		}
		return kDefaultOrderByPushdown;
	}

	// Fewer than one thread is not a scan; cap the top so a stray setting
	// cannot open an unbounded number of connections.
	static int64_t NormalizeMaxThreads(const Value &value) {
		auto threads = BigIntValue::Get(value);
		if (threads < 1) {
			return 1;
		}
		return threads > 64 ? 64 : threads;
	}

	// Any negative value means "sample everything"; 0 would infer an empty
	// schema, so treat it the same way rather than silently returning no columns.
	static int64_t NormalizeSampleSize(const Value &value) {
		auto sample = BigIntValue::Get(value);
		return sample <= 0 ? kSampleAllDocuments : sample;
	}

private:
	static int64_t NormalizeTTL(const Value &value) {
		auto ttl = BigIntValue::Get(value);
		return ttl < 0 ? 0 : ttl;
	}
};

} // namespace duckdb
