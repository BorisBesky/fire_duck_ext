#include "firestore_path_utils.hpp"

namespace duckdb {

int CountFirestorePathSegments(const std::string &path) {
	int count = 0;
	bool in_segment = false;
	for (char c : path) {
		if (c == '/') {
			in_segment = false;
		} else if (!in_segment) {
			in_segment = true;
			count++;
		}
	}
	return count;
}

bool IsFirestoreDocumentPath(const std::string &path) {
	int segments = CountFirestorePathSegments(path);
	return segments >= 2 && segments % 2 == 0;
}

bool IsFirestoreDocumentPathCollection(const std::string &collection) {
	if (!collection.empty() && collection[0] == '~') {
		return false;
	}
	return IsFirestoreDocumentPath(collection);
}

} // namespace duckdb
