#pragma once

// Minimal self-registering test harness.
//
// The DuckDB-free modules (firestore_wire, firestore_paging,
// firestore_schema_accumulator) are compiled straight into this binary, so
// their branches can be driven directly and measured with gcov. Anything
// needing a live DuckDB lives in test/sql instead.

#include <exception>
#include <functional>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace fdtest {

struct Failure : std::exception {
	explicit Failure(std::string message) : message(std::move(message)) {
	}
	const char *what() const noexcept override {
		return message.c_str();
	}
	std::string message;
};

struct TestCase {
	std::string name;
	std::function<void()> body;
};

std::vector<TestCase> &Registry();

struct Registrar {
	Registrar(const char *name, std::function<void()> body) {
		Registry().push_back(TestCase {name, std::move(body)});
	}
};

// Whether a value of type T can be written to an ostream.
template <class T, class = void>
struct IsStreamable : std::false_type {};

template <class T>
struct IsStreamable<T, std::void_t<decltype(std::declval<std::ostream &>() << std::declval<const T &>())>>
    : std::true_type {};

// Renders a value for a failure message. Falls back to a placeholder for
// types with no stream operator, so an assertion on any comparable type
// compiles -- the failure branch that calls this has to compile even in the
// tests that never take it.
template <class T>
std::string Describe(const T &value) {
	if constexpr (IsStreamable<T>::value) {
		std::ostringstream out;
		out << value;
		return out.str();
	} else {
		(void)value;
		return "<no operator<< for this type>";
	}
}
inline std::string Describe(bool value) {
	return value ? "true" : "false";
}
inline std::string Describe(std::nullptr_t) {
	return "nullptr";
}

int RunAll(const std::string &filter);

} // namespace fdtest

#define FD_CONCAT_INNER(a, b) a##b
#define FD_CONCAT(a, b)       FD_CONCAT_INNER(a, b)

#define FD_TEST(name)                                                                                                  \
	static void FD_CONCAT(fd_test_body_, __LINE__)();                                                                  \
	static ::fdtest::Registrar FD_CONCAT(fd_test_reg_, __LINE__)(name, FD_CONCAT(fd_test_body_, __LINE__));            \
	static void FD_CONCAT(fd_test_body_, __LINE__)()

#define FD_FAIL(detail)                                                                                                \
	throw ::fdtest::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " + (detail))

#define FD_REQUIRE(expr)                                                                                               \
	do {                                                                                                               \
		if (!(expr)) {                                                                                                 \
			FD_FAIL(std::string("expected true: ") + #expr);                                                           \
		}                                                                                                              \
	} while (false)

#define FD_REQUIRE_FALSE(expr)                                                                                         \
	do {                                                                                                               \
		if ((expr)) {                                                                                                  \
			FD_FAIL(std::string("expected false: ") + #expr);                                                          \
		}                                                                                                              \
	} while (false)

#define FD_REQUIRE_EQ(actual, expected)                                                                                \
	do {                                                                                                               \
		const auto &fd_actual = (actual);                                                                              \
		const auto &fd_expected = (expected);                                                                          \
		if (!(fd_actual == fd_expected)) {                                                                             \
			FD_FAIL(std::string(#actual) + " == " + #expected + "\n         actual: " +                                \
			        ::fdtest::Describe(fd_actual) + "\n       expected: " + ::fdtest::Describe(fd_expected));          \
		}                                                                                                              \
	} while (false)
