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
#include <sstream>
#include <string>
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

// Renders a value for a failure message. Falls back to a placeholder for
// types with no stream operator so the harness never fails to compile.
template <class T>
std::string Describe(const T &value) {
	std::ostringstream out;
	out << value;
	return out.str();
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
