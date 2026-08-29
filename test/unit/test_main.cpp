#include "test_harness.hpp"

namespace fdtest {

std::vector<TestCase> &Registry() {
	static std::vector<TestCase> registry;
	return registry;
}

int RunAll(const std::string &filter) {
	int passed = 0;
	std::vector<std::string> failures;

	for (const auto &test : Registry()) {
		if (!filter.empty() && test.name.find(filter) == std::string::npos) {
			continue;
		}
		try {
			test.body();
			passed++;
		} catch (const Failure &failure) {
			failures.push_back(test.name + "\n    " + failure.message);
		} catch (const std::exception &error) {
			failures.push_back(test.name + "\n    unexpected exception: " + error.what());
		} catch (...) {
			failures.push_back(test.name + "\n    unexpected non-standard exception");
		}
	}

	for (const auto &failure : failures) {
		std::cout << "FAIL  " << failure << "\n";
	}
	std::cout << passed << " passed, " << failures.size() << " failed" << std::endl;
	return failures.empty() ? 0 : 1;
}

} // namespace fdtest

int main(int argc, char **argv) {
	return fdtest::RunAll(argc > 1 ? argv[1] : "");
}
