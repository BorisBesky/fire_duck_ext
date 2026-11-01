#pragma once

// Retrying transient Firestore failures.
//
// Firestore sheds load with 429 RESOURCE_EXHAUSTED and 503 UNAVAILABLE,
// aborts contended writes with 409 ABORTED, and now and then fails a request
// outright; Google's client libraries retry all of these with exponential
// backoff, and so does FirestoreClient::MakeRequest. This module holds the
// decisions -- which failures to retry, and how long to wait -- and has no
// DuckDB dependency, so they are unit tested without a network
// (test/unit/test_firestore_retry.cpp).

#include <cstdint>
#include <string>

namespace duckdb {

struct FirestoreRetryPolicy {
	// Defaults sized for a scan that should ride out a brief outage or a
	// quota spike: with equal jitter the waits are 0.5-1s, 1-2s, 2-4s, 4-8s
	// and 8-16s, so the last attempt goes out 15.5-31s after the first.
	static constexpr int64_t kDefaultMaxRetries = 5;
	static constexpr int64_t kDefaultInitialBackoffMs = 1000;
	static constexpr int64_t kDefaultMaxBackoffMs = 32000;

	// Attempts after the first; 0 disables retrying.
	int64_t max_retries = kDefaultMaxRetries;
	// Upper end of the first wait; each further retry doubles it.
	int64_t initial_backoff_ms = kDefaultInitialBackoffMs;
	// Ceiling on any one wait, Retry-After included.
	int64_t max_backoff_ms = kDefaultMaxBackoffMs;
};

// Whether sending a request twice has the same effect as sending it once.
// A failure can leave it unknown whether Firestore applied the request, and
// only a request for which that does not matter is repeated after one.
enum class FirestoreRetrySafety : uint8_t {
	// Reads, deletes, and writes that set fields to given values.
	IDEMPOTENT,
	// Creating a document: a repeat makes a second document (auto id) or
	// fails with ALREADY_EXISTS (given id).
	NOT_IDEMPOTENT
};

// How an attempt that produced no HTTP response failed.
enum class FirestoreTransportFailure : uint8_t {
	// A response arrived, whatever its status.
	NONE,
	// No connection was made, so the request never left this process.
	NOT_SENT,
	// The connection failed after the request may have been sent.
	AMBIGUOUS,
	// Would fail the same way however often it is tried: a certificate that
	// does not verify, say.
	PERMANENT
};

// Whether a response is worth asking for again. `rpc_status` is the
// google.rpc.Code name Firestore puts in error.status ("UNAVAILABLE", ...),
// or empty when the body carried none -- a proxy's HTML error page, say --
// in which case the HTTP status decides.
bool ShouldRetryFirestoreResponse(int http_status, const std::string &rpc_status, FirestoreRetrySafety safety);

// Whether an attempt that got no response is worth repeating.
bool ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure failure, FirestoreRetrySafety safety);

// Whether one write's status in a batchWrite response is transient. Each
// write in a batch succeeds or fails on its own, and a failed one was not
// applied, so it can be resent whatever it does. `code` is a google.rpc.Code
// number.
bool IsTransientFirestoreRpcCode(int code);

// A Retry-After header in milliseconds, or -1 when it is absent or not a
// whole number of seconds (the HTTP-date form is not used by Google APIs).
int64_t ParseRetryAfterMs(const std::string &header);

// Milliseconds to wait before retry number `retry` (0 for the first).
// "Equal jitter": half of an exponentially growing window plus a random part
// of the other half, so concurrent clients spread out without any of them
// retrying immediately. `jitter` is uniform in [0, 1). A Retry-After the
// server sent (`retry_after_ms` >= 0) raises the wait to at least that.
// Either way the wait never exceeds max_backoff_ms.
int64_t FirestoreRetryDelayMs(const FirestoreRetryPolicy &policy, int64_t retry, double jitter,
                              int64_t retry_after_ms = -1);

// Settings normalisation: a negative retry count or backoff means 0, and the
// ceiling is never below the initial backoff.
int64_t NormalizeFirestoreRetryCount(int64_t retries);
int64_t NormalizeFirestoreBackoffMs(int64_t backoff_ms);
FirestoreRetryPolicy NormalizeFirestoreRetryPolicy(FirestoreRetryPolicy policy);

} // namespace duckdb
