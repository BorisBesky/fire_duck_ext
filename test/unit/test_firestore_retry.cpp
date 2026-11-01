#include "test_harness.hpp"
#include "firestore_retry.hpp"

#include <cmath>
#include <cstdint>
#include <limits>

using duckdb::FirestoreRetryPolicy;
using duckdb::FirestoreRetrySafety;
using duckdb::FirestoreTransportFailure;

namespace {

constexpr auto kIdempotent = FirestoreRetrySafety::IDEMPOTENT;
constexpr auto kNotIdempotent = FirestoreRetrySafety::NOT_IDEMPOTENT;

FirestoreRetryPolicy Policy(int64_t retries, int64_t initial_ms, int64_t max_ms) {
	FirestoreRetryPolicy policy;
	policy.max_retries = retries;
	policy.initial_backoff_ms = initial_ms;
	policy.max_backoff_ms = max_ms;
	return policy;
}

} // namespace

// ---------------------------------------------------------------- responses

FD_TEST("retry: a success is never retried") {
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(200, "", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(204, "UNAVAILABLE", kIdempotent));
}

FD_TEST("retry: refusals that applied nothing are retried for any request") {
	for (auto safety : {kIdempotent, kNotIdempotent}) {
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(429, "RESOURCE_EXHAUSTED", safety));
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(503, "UNAVAILABLE", safety));
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(409, "ABORTED", safety));
	}
}

FD_TEST("retry: failures that may have applied the request are retried only when repeating is harmless") {
	FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(500, "INTERNAL", kIdempotent));
	FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(504, "DEADLINE_EXCEEDED", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(500, "INTERNAL", kNotIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(504, "DEADLINE_EXCEEDED", kNotIdempotent));
}

FD_TEST("retry: Firestore's status overrides the HTTP status") {
	// 409 is also ALREADY_EXISTS, which a retry cannot fix.
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(409, "ALREADY_EXISTS", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(400, "INVALID_ARGUMENT", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(403, "PERMISSION_DENIED", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(404, "NOT_FOUND", kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(500, "UNKNOWN", kIdempotent));
	// An unrecognised status is not guessed at, whatever the HTTP status.
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(503, "SOMETHING_NEW", kIdempotent));
}

FD_TEST("retry: without a Firestore status the HTTP status decides") {
	for (auto safety : {kIdempotent, kNotIdempotent}) {
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(429, "", safety));
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(503, "", safety));
	}
	for (int status : {408, 500, 502, 504}) {
		FD_REQUIRE(duckdb::ShouldRetryFirestoreResponse(status, "", kIdempotent));
		FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(status, "", kNotIdempotent));
	}
	for (int status : {400, 401, 403, 404, 409, 501}) {
		FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreResponse(status, "", kIdempotent));
	}
}

// ---------------------------------------------------------------- transport

FD_TEST("retry: a request that was never sent is retried for any request") {
	FD_REQUIRE(duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::NOT_SENT, kIdempotent));
	FD_REQUIRE(duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::NOT_SENT, kNotIdempotent));
}

FD_TEST("retry: a connection lost mid-request is retried only when repeating is harmless") {
	FD_REQUIRE(duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::AMBIGUOUS, kIdempotent));
	FD_REQUIRE_FALSE(
	    duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::AMBIGUOUS, kNotIdempotent));
}

FD_TEST("retry: permanent transport failures and responses are not transport retries") {
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::PERMANENT, kIdempotent));
	FD_REQUIRE_FALSE(duckdb::ShouldRetryFirestoreTransportFailure(FirestoreTransportFailure::NONE, kIdempotent));
}

// ---------------------------------------------------------------- batch statuses

FD_TEST("retry: transient per-write codes in a batchWrite response") {
	for (int code : {4, 8, 10, 13, 14}) {
		FD_REQUIRE(duckdb::IsTransientFirestoreRpcCode(code));
	}
	// OK, INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED,
	// FAILED_PRECONDITION, UNKNOWN.
	for (int code : {0, 3, 5, 6, 7, 9, 2}) {
		FD_REQUIRE_FALSE(duckdb::IsTransientFirestoreRpcCode(code));
	}
}

// ---------------------------------------------------------------- Retry-After

FD_TEST("retry: Retry-After in seconds becomes milliseconds") {
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("0"), 0);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("7"), 7000);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("  12 \t"), 12000);
}

FD_TEST("retry: an absent or unusable Retry-After is -1") {
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs(""), -1);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("   "), -1);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("-5"), -1);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("1.5"), -1);
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("Wed, 21 Oct 2015 07:28:00 GMT"), -1);
}

FD_TEST("retry: a huge Retry-After saturates at a day instead of overflowing") {
	FD_REQUIRE_EQ(duckdb::ParseRetryAfterMs("99999999999999999999999999"), int64_t(24 * 60 * 60) * 1000);
}

// ---------------------------------------------------------------- delays

FD_TEST("retry: waits double from the initial backoff, between half and all of the window") {
	auto policy = Policy(5, 1000, 32000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 0.0), 500);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 0.5), 750);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 1, 0.0), 1000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 2, 0.0), 2000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 4, 0.0), 8000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 4, 0.999999), 15999);
}

FD_TEST("retry: the window stops growing at the ceiling") {
	auto policy = Policy(100, 1000, 32000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 5, 1.0), 32000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 6, 0.0), 16000);
	// A retry number far past the ceiling neither overflows nor loops for long.
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, std::numeric_limits<int64_t>::max(), 1.0), 32000);
	auto huge = Policy(3, std::numeric_limits<int64_t>::max() / 2 + 1, std::numeric_limits<int64_t>::max());
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(huge, 3, 1.0), std::numeric_limits<int64_t>::max());
}

FD_TEST("retry: Retry-After lengthens a wait but never past the ceiling") {
	auto policy = Policy(5, 1000, 32000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 0.0, 5000), 5000);
	// A shorter Retry-After does not cut the backoff.
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 2, 0.0, 100), 2000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 0.0, 3600 * 1000), 32000);
}

FD_TEST("retry: jitter outside [0, 1] is clamped") {
	auto policy = Policy(5, 1000, 32000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, -3.0), 500);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, std::nan("")), 500);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 7.0), 1000);
}

FD_TEST("retry: a zero backoff retries immediately") {
	auto policy = Policy(5, 0, 0);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 0, 0.5), 0);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(policy, 3, 0.5), 0);
}

// ---------------------------------------------------------------- normalisation

FD_TEST("retry: negative settings mean zero") {
	FD_REQUIRE_EQ(duckdb::NormalizeFirestoreRetryCount(-1), 0);
	FD_REQUIRE_EQ(duckdb::NormalizeFirestoreRetryCount(3), 3);
	FD_REQUIRE_EQ(duckdb::NormalizeFirestoreBackoffMs(-250), 0);
	FD_REQUIRE_EQ(duckdb::NormalizeFirestoreBackoffMs(250), 250);
}

FD_TEST("retry: the ceiling is raised to the initial backoff") {
	auto policy = duckdb::NormalizeFirestoreRetryPolicy(Policy(-2, 5000, 100));
	FD_REQUIRE_EQ(policy.max_retries, 0);
	FD_REQUIRE_EQ(policy.initial_backoff_ms, 5000);
	FD_REQUIRE_EQ(policy.max_backoff_ms, 5000);
	FD_REQUIRE_EQ(duckdb::FirestoreRetryDelayMs(Policy(1, 5000, 100), 0, 1.0), 5000);
}

FD_TEST("retry: the defaults") {
	FirestoreRetryPolicy policy;
	FD_REQUIRE_EQ(policy.max_retries, 5);
	FD_REQUIRE_EQ(policy.initial_backoff_ms, 1000);
	FD_REQUIRE_EQ(policy.max_backoff_ms, 32000);
}
