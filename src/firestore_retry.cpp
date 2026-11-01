#include "firestore_retry.hpp"

#include <algorithm>
#include <cctype>

namespace duckdb {

bool ShouldRetryFirestoreResponse(int http_status, const std::string &rpc_status, FirestoreRetrySafety safety) {
	if (http_status >= 200 && http_status < 300) {
		return false;
	}
	const bool idempotent = safety == FirestoreRetrySafety::IDEMPOTENT;

	// Firestore's own status is the more precise of the two: HTTP 409 is both
	// ABORTED, which is retryable, and ALREADY_EXISTS, which is not.
	if (!rpc_status.empty()) {
		// Refused before anything was applied, so any request may be resent.
		if (rpc_status == "RESOURCE_EXHAUSTED" || rpc_status == "UNAVAILABLE" || rpc_status == "ABORTED") {
			return true;
		}
		// The request may have been applied before it failed.
		if (rpc_status == "DEADLINE_EXCEEDED" || rpc_status == "INTERNAL") {
			return idempotent;
		}
		return false;
	}

	// No Firestore status: the error came from something in between, such as
	// a load balancer, so judge by the HTTP status the same way.
	switch (http_status) {
	case 429:
	case 503:
		return true;
	case 408:
	case 500:
	case 502:
	case 504:
		return idempotent;
	default:
		return false;
	}
}

bool ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure failure, FirestoreRetrySafety safety) {
	switch (failure) {
	case FirestoreTransportFailure::NOT_SENT:
		return true;
	case FirestoreTransportFailure::AMBIGUOUS:
		return safety == FirestoreRetrySafety::IDEMPOTENT;
	case FirestoreTransportFailure::NONE:
	case FirestoreTransportFailure::PERMANENT:
	default:
		return false;
	}
}

bool IsTransientFirestoreRpcCode(int code) {
	switch (code) {
	case 4:  // DEADLINE_EXCEEDED
	case 8:  // RESOURCE_EXHAUSTED
	case 10: // ABORTED
	case 13: // INTERNAL
	case 14: // UNAVAILABLE
		return true;
	default:
		return false;
	}
}

int64_t ParseRetryAfterMs(const std::string &header) {
	auto begin = header.find_first_not_of(" \t");
	if (begin == std::string::npos) {
		return -1;
	}
	auto end = header.find_last_not_of(" \t");

	// A day is far beyond any wait worth honouring, and stopping there keeps
	// the arithmetic clear of overflow; the policy's ceiling applies anyway.
	constexpr int64_t kMaxSeconds = 24 * 60 * 60;
	int64_t seconds = 0;
	for (auto i = begin; i <= end; i++) {
		auto c = static_cast<unsigned char>(header[i]);
		if (!std::isdigit(c)) {
			return -1;
		}
		seconds = std::min<int64_t>(seconds * 10 + (c - '0'), kMaxSeconds);
	}
	return seconds * 1000;
}

int64_t FirestoreRetryDelayMs(const FirestoreRetryPolicy &policy, int64_t retry, double jitter,
                              int64_t retry_after_ms) {
	auto normalized = NormalizeFirestoreRetryPolicy(policy);

	// initial * 2^retry, capped, doubling step by step so a large retry
	// number cannot overflow.
	int64_t window = normalized.initial_backoff_ms;
	for (int64_t i = 0; i < retry && window > 0 && window < normalized.max_backoff_ms; i++) {
		window = window > normalized.max_backoff_ms / 2 ? normalized.max_backoff_ms : window * 2;
	}
	window = std::min(window, normalized.max_backoff_ms);

	if (!(jitter >= 0)) { // also catches NaN
		jitter = 0;
	}
	jitter = std::min(jitter, 1.0);
	const int64_t half = window / 2;
	int64_t delay = half + static_cast<int64_t>(static_cast<double>(window - half) * jitter);

	delay = std::max(delay, retry_after_ms);
	return std::min(delay, normalized.max_backoff_ms);
}

int64_t NormalizeFirestoreRetryCount(int64_t retries) {
	return std::max<int64_t>(retries, 0);
}

int64_t NormalizeFirestoreBackoffMs(int64_t backoff_ms) {
	return std::max<int64_t>(backoff_ms, 0);
}

FirestoreRetryPolicy NormalizeFirestoreRetryPolicy(FirestoreRetryPolicy policy) {
	policy.max_retries = NormalizeFirestoreRetryCount(policy.max_retries);
	policy.initial_backoff_ms = NormalizeFirestoreBackoffMs(policy.initial_backoff_ms);
	policy.max_backoff_ms = std::max(NormalizeFirestoreBackoffMs(policy.max_backoff_ms), policy.initial_backoff_ms);
	return policy;
}

} // namespace duckdb
