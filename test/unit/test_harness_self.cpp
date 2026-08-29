// Self-tests for the test harness itself.
//
// Describe() promises that an assertion on any comparable type compiles, even
// one with no operator<<. That promise lives in a branch the passing tests
// never take, so without a test for it the harness could stop honouring it
// without anything noticing.

#include "test_harness.hpp"
#include <string>

namespace {

// Comparable, deliberately not streamable.
struct Opaque {
	int value;
	bool operator==(const Opaque &other) const {
		return value == other.value;
	}
};

struct Streamable {
	int value;
	bool operator==(const Streamable &other) const {
		return value == other.value;
	}
};

std::ostream &operator<<(std::ostream &out, const Streamable &streamable) {
	return out << "Streamable(" << streamable.value << ")";
}

} // namespace

FD_TEST("harness: streamable types are rendered through operator<<") {
	FD_REQUIRE(fdtest::IsStreamable<Streamable>::value);
	FD_REQUIRE_EQ(fdtest::Describe(Streamable {7}), std::string("Streamable(7)"));
	FD_REQUIRE_EQ(fdtest::Describe(42), std::string("42"));
	FD_REQUIRE_EQ(fdtest::Describe(std::string("text")), std::string("text"));
}

FD_TEST("harness: bool and nullptr get readable renderings") {
	// Without the overload these stream as 1/0, which reads badly in a
	// failure message.
	FD_REQUIRE_EQ(fdtest::Describe(true), std::string("true"));
	FD_REQUIRE_EQ(fdtest::Describe(false), std::string("false"));
	FD_REQUIRE_EQ(fdtest::Describe(nullptr), std::string("nullptr"));
}

FD_TEST("harness: a type with no operator<< falls back to a placeholder") {
	FD_REQUIRE_FALSE(fdtest::IsStreamable<Opaque>::value);
	FD_REQUIRE_EQ(fdtest::Describe(Opaque {1}), std::string("<no operator<< for this type>"));

	// The real point: this assertion's failure branch calls Describe, so it
	// has to compile for a type that cannot be streamed.
	FD_REQUIRE_EQ(Opaque {3}, Opaque {3});
}

FD_TEST("harness: a failing assertion reports both values") {
	bool threw = false;
	try {
		FD_REQUIRE_EQ(1, 2);
	} catch (const fdtest::Failure &failure) {
		threw = true;
		FD_REQUIRE(failure.message.find("actual: 1") != std::string::npos);
		FD_REQUIRE(failure.message.find("expected: 2") != std::string::npos);
	}
	FD_REQUIRE(threw);
}

FD_TEST("harness: the boolean assertions throw on the failing case") {
	bool threw = false;
	try {
		FD_REQUIRE(false);
	} catch (const fdtest::Failure &) {
		threw = true;
	}
	FD_REQUIRE(threw);

	threw = false;
	try {
		FD_REQUIRE_FALSE(true);
	} catch (const fdtest::Failure &) {
		threw = true;
	}
	FD_REQUIRE(threw);
}
